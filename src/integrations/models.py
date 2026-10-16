import uuid

from django.conf import settings
from django.db import models
from django.utils import timezone


class ImportPlanGeneration(models.Model):
    """One generation of a periodic import plan.

    The primary key is embedded into every message Beat publishes for the plan,
    so the broker message, its TaskResult and the worker writes all carry the
    generation that was active when the plan was created. Deleting a plan closes
    its generation; messages carrying a closed generation terminate before they
    can write to the media library or refresh the calendar. Recreating a plan
    with the same name creates a new generation, so old messages never belong to
    the new plan.
    """

    class Status(models.TextChoices):
        """Lifecycle states of an import plan generation."""

        ACTIVE = "active", "Active"
        CLOSED = "closed", "Closed"

    generation_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )
    # Celery task name, e.g. "Import from Trakt".
    task_name = models.CharField(max_length=200)
    # Snapshot of the PeriodicTask name when the generation was created.
    periodic_task_name = models.CharField(max_length=200)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="import_generations",
    )
    status = models.CharField(
        max_length=10,
        choices=Status.choices,
        default=Status.ACTIVE,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    closed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        """Table options."""

        indexes = [
            models.Index(fields=["user", "status"]),
            models.Index(fields=["status"]),
        ]
        ordering = ["-created_at"]

    def __str__(self):
        """Human-readable generation identifier."""
        return f"{self.periodic_task_name} [{self.status}] ({self.generation_id})"

    @property
    def is_active(self):
        """Return whether the generation still accepts published messages."""
        return self.status == self.Status.ACTIVE

    def close(self):
        """Close the generation, recording when it happened."""
        self.status = self.Status.CLOSED
        self.closed_at = timezone.now()
        self.save(update_fields=["status", "closed_at"])
