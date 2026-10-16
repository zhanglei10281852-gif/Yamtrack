"""Stable, persistent generations for periodic import plans.

A plan generation is identified by a UUID that Beat embeds into every published
message (task kwargs and headers). The id therefore reaches the worker and the
TaskResult row created on publish, tying the whole execution to the plan as it
existed when the message was created.

Lifecycle and ownership rules:

* ``preflight`` runs before any side effect of an import task: a message whose
  generation no longer exists or is closed terminates without touching media or
  refreshing the calendar.
* ``acquire_write_ownership`` runs inside the single write transaction, right
  before the first production write. It row-locks the generation and rechecks
  its status, closing the race between plan deletion and a running import.
* ``close_generation`` marks a generation closed and removes its PeriodicTask in
  one transaction. If a running task already owns the write lock the close
  fails immediately and the plan keeps running with its original semantics.
* Broker revocations are best effort (Redis does not persist them); the
  database generation checks are the authoritative guard and also cover
  messages redelivered after a worker restart.
"""

import contextvars
import json
import logging

from celery import current_task, states
from celery.exceptions import Ignore
from django.db import DatabaseError, connection, transaction
from django.utils import timezone
from django_celery_beat.models import PeriodicTask
from django_celery_results.models import TaskResult

from config.celery import app as celery_app
from integrations.models import ImportPlanGeneration

logger = logging.getLogger(__name__)

# Kwarg/header carrying the generation id in messages published by Beat.
PLAN_GENERATION_KWARG = "plan_generation"

TERMINATED_MESSAGE = "Import stopped because its schedule was deleted."

# Only import tasks are ever bound to a plan generation.
IMPORT_TASK_PREFIX = "Import from "

_current_generation = contextvars.ContextVar(
    "import_plan_generation",
    default=None,
)


class ImportGenerationClosedError(Exception):
    """A closed generation reached the production write zone."""


class PlanWriteInProgressError(DatabaseError):
    """The plan cannot be closed while one of its runs owns the write zone."""


def generation_repr(generation_id):
    """Return how the generation id appears inside TaskResult.task_kwargs.

    Celery stores ``kwargsrepr`` as the repr() of the kwargs dict, so a string
    value renders as ``'plan_generation': '<uuid>'``.
    """
    return f"'{PLAN_GENERATION_KWARG}': '{generation_id}'"


def parse_generation_id(task_kwargs):
    """Extract the generation id from a PeriodicTask kwargs JSON blob."""
    try:
        kwargs = json.loads(task_kwargs or "{}")
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(kwargs, dict):
        return None
    generation_id = kwargs.get(PLAN_GENERATION_KWARG)
    return str(generation_id) if generation_id else None


def generation_is_active(generation_id):
    """Return whether an existing, open generation accepts new writes."""
    if not generation_id:
        return False
    return ImportPlanGeneration.objects.filter(
        generation_id=generation_id,
        status=ImportPlanGeneration.Status.ACTIVE,
    ).exists()


def bind_current_generation(generation_id):
    """Bind the generation of the running task to the current context."""
    return _current_generation.set(generation_id)


def reset_current_generation(token):
    """Restore the import generation context after a task finishes."""
    _current_generation.reset(token)


def current_generation_id():
    """Return the generation id bound to the current task, if any."""
    return _current_generation.get()


def preflight(generation_id):
    """Terminate before any side effect when the plan was deleted.

    One-time imports (no generation) and open plans pass through unchanged.
    """
    if generation_id is not None and not generation_is_active(generation_id):
        logger.info(
            "Skipping import for closed or missing plan generation %s",
            generation_id,
        )
        terminate_current_task()


def acquire_write_ownership(generation_id):
    """Lock and revalidate the generation at the start of the write zone.

    Must be called inside ``transaction.atomic()``. The row lock serializes the
    writer against plan deletion: a concurrent close either rolled back
    (generation still active, write proceeds) or committed (generation closed,
    the import terminates and the surrounding transaction rolls back).
    """
    if generation_id is None:
        return

    generation = (
        ImportPlanGeneration.objects.select_for_update()
        .filter(generation_id=generation_id)
        .first()
    )
    if generation is None or not generation.is_active:
        raise ImportGenerationClosedError(generation_id)


def terminate_current_task(message=TERMINATED_MESSAGE):
    """Mark the current task's result REVOKED and stop without a failure.

    Raising Ignore tells Celery not to overwrite the result state, so the
    REVOKED row persists and no calendar refresh or success handling runs.
    """
    request = current_task.request
    TaskResult.objects.store_result(
        content_type="application/json",
        content_encoding="utf-8",
        task_id=request.id,
        result=json.dumps(message),
        status=states.REVOKED,
        task_name=request.task,
        task_args=getattr(request, "argsrepr", ""),
        task_kwargs=getattr(request, "kwargsrepr", ""),
    )
    raise Ignore


def close_generation(periodic_task):
    """Close the generation of a plan and delete its PeriodicTask.

    The status flip and the schedule deletion share one transaction, so a
    failure leaves the original plan valid and scheduled. Raises
    PlanWriteInProgressError when an import run currently owns the write zone,
    in which case nothing is changed.

    Plans created before generations existed (no id in kwargs) simply lose
    their PeriodicTask, preserving the legacy deletion behavior.
    """
    generation_id = parse_generation_id(periodic_task.kwargs)
    with transaction.atomic():
        if generation_id is not None:
            try:
                generation = (
                    ImportPlanGeneration.objects.select_for_update(
                        nowait=connection.features.has_select_for_update_nowait,
                    )
                    .get(generation_id=generation_id)
                )
            except ImportPlanGeneration.DoesNotExist:
                # Generation row is gone but the plan row lingered; deleting the
                # row still closes the plan because no open generation accepts it.
                logger.warning(
                    "Periodic task %s has no generation %s, deleting it anyway",
                    periodic_task.name,
                    generation_id,
                )
            except DatabaseError as exc:
                raise PlanWriteInProgressError(generation_id) from exc
            else:
                generation.close()

        periodic_task.delete()
    return generation_id


def revoke_pending_runs(generation_id):
    """Best-effort cancel of messages already published for a closed plan.

    Queued and started TaskResult rows are marked REVOKED so they cannot linger
    or be mistaken for runs of a recreated plan, and queued broker messages are
    revoked when the control channel is available. A started task cannot have
    entered the write zone (a successful close is refused while a writer holds
    the generation lock); it terminates itself at the write gate instead of
    being killed mid-write.
    """
    if generation_id is None:
        return

    pending_runs = TaskResult.objects.filter(
        status__in=(states.PENDING, states.STARTED),
        task_name__startswith=IMPORT_TASK_PREFIX,
        task_kwargs__contains=generation_repr(generation_id),
    )
    task_ids = list(pending_runs.values_list("task_id", flat=True))
    if not task_ids:
        return

    pending_runs.update(
        status=states.REVOKED,
        result=json.dumps(TERMINATED_MESSAGE),
        date_done=timezone.now(),
    )

    try:
        celery_app.control.revoke(task_ids)
    except Exception:
        # Revocation is an optimization; the database gates are authoritative,
        # including after a broker or control-plane restart.
        logger.warning(
            "Could not revoke broker messages for generation %s",
            generation_id,
            exc_info=True,
        )


def open_generation(*, user, task_name, periodic_task_name):
    """Create a new active generation for a freshly created plan."""
    return ImportPlanGeneration.objects.create(
        user=user,
        task_name=task_name,
        periodic_task_name=periodic_task_name,
    )


def get_periodic_task_for_user(task_name, user):
    """Return the import plan with the given name owned by the user."""
    return PeriodicTask.objects.get(
        name=task_name,
        kwargs__contains=f'"user_id": {user.id}',
    )
