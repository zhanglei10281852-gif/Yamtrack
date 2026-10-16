import logging

from django.db import IntegrityError, transaction
from django.db.models import F

from events.models import CalendarRevision

logger = logging.getLogger(__name__)


def reserve_generation(item):
    """Reserve the next refresh generation for a media entry.

    Called before any provider data is fetched so that generations reflect the
    order in which refresh rounds started, even when the earlier round
    finishes after a newer one. The increment is a single row-locked UPDATE,
    which serializes concurrent rounds (manual refresh, media status changes,
    import completion and the periodic task) per media entry on PostgreSQL.
    """
    with transaction.atomic():
        updated = CalendarRevision.objects.filter(item_id=item.id).update(
            generation=F("generation") + 1,
        )

        if not updated:
            # No marker row yet. The nested savepoint keeps the outer
            # transaction usable if another process inserts the same row first.
            try:
                with transaction.atomic():
                    CalendarRevision.objects.create(item_id=item.id, generation=1)
            except IntegrityError:
                logger.info(
                    "Calendar revision for item %s was created concurrently",
                    item.id,
                )
                CalendarRevision.objects.filter(item_id=item.id).update(
                    generation=F("generation") + 1,
                )

        return CalendarRevision.objects.get(item_id=item.id).generation
