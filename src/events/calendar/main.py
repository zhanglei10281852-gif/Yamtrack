import logging

from django.db import transaction

from app.models import MediaTypes, Sources
from events.models import CalendarRevision, Event

from .anime import process_anime_bulk
from .comic import process_comic
from .other import process_other
from .revisions import reserve_generation
from .selectors import get_items_to_process
from .tv import process_tv

logger = logging.getLogger(__name__)


def fetch_releases(user=None, items_to_process=None):
    """Fetch and process releases for the calendar."""
    if items_to_process and items_to_process[0].source == Sources.MANUAL.value:
        return "Manual sources are not processed"

    items_to_process = items_to_process or get_items_to_process(user)
    if not items_to_process:
        return "No items to process"

    logger.info(
        "Processing %d items:\n%s",
        len(items_to_process),
        format_items(items_to_process),
    )

    # Generations are reserved before any provider fetch so that a round's
    # priority reflects its start time: an earlier round that finishes later
    # will see a newer generation and yield instead of committing a stale
    # snapshot.
    generations = reserve_refresh_round(items_to_process)
    events_bulk = process_items(items_to_process)
    items_updated = commit_snapshots(items_to_process, events_bulk, generations)

    return generate_final_message(items_to_process, items_updated)


def reserve_refresh_round(items):
    """Reserve a per-entry generation for every item in this refresh round."""
    return {item.id: reserve_generation(item) for item in items}


def group_events_by_entry(entries, events_bulk):
    """Map every produced event back to its processed media entry.

    TV entries carry their events on the season items, so season events are
    attributed to the TV entry identified by media id and source. Every other
    media type owns its events directly.
    """
    tv_entry_ids = {
        (item.media_id, item.source): item.id
        for item in entries
        if item.media_type == MediaTypes.TV.value
    }

    events_by_entry = {}
    for event in events_bulk:
        owner = event.item
        if owner.media_type == MediaTypes.SEASON.value:
            entry_id = tv_entry_ids.get((owner.media_id, owner.source))
            if entry_id is None:
                logger.warning(
                    "Skipping season event without a processed TV entry: %s",
                    owner,
                )
                continue
        else:
            entry_id = owner.id

        events_by_entry.setdefault(entry_id, []).append(event)

    return events_by_entry


def commit_snapshots(entries, events_bulk, generations):
    """Commit complete per-entry snapshots, newest generation wins."""
    events_by_entry = group_events_by_entry(entries, events_bulk)
    items_updated = set()

    for entry in entries:
        entry_events = events_by_entry.get(entry.id)
        if not entry_events:
            # Nothing was produced for this entry (provider failure or no new
            # releases): leave the previous complete calendar in place and let
            # later refreshes take over.
            continue

        if commit_entry_snapshot(entry, entry_events, generations[entry.id]):
            items_updated.update(event.item for event in entry_events)

    return items_updated


def commit_entry_snapshot(entry, entry_events, generation):
    """Atomically commit one entry's snapshot when the round is still latest.

    The generation check, event upserts, stale-event cleanup and committed
    marker update run in a single transaction, so an interrupted commit can
    never expose half-new, half-old events. Returns ``False`` (without any
    write) when a newer round has taken over.
    """
    with transaction.atomic():
        try:
            revision = CalendarRevision.objects.select_for_update().get(
                item_id=entry.id,
            )
        except CalendarRevision.DoesNotExist:
            logger.warning(
                "No calendar revision found for %s; skipping snapshot commit",
                entry,
            )
            return False

        if revision.generation != generation:
            logger.info(
                "Skipping stale snapshot for %s: round generation %s, latest %s",
                entry,
                generation,
                revision.generation,
            )
            return False

        if revision.committed_generation >= generation:
            logger.info(
                "Skipping already committed snapshot for %s (generation %s)",
                entry,
                generation,
            )
            return False

        save_events(entry_events)
        cleanup_invalid_events(entry_events)

        # Conditional final claim: even on backends without row locks, a newer
        # round committing here forces the whole snapshot to roll back.
        claimed = CalendarRevision.objects.filter(
            item_id=entry.id,
            generation=generation,
            committed_generation__lt=generation,
        ).update(committed_generation=generation)

        if not claimed:
            logger.warning(
                "Lost commit race for %s at generation %s; rolling back",
                entry,
                generation,
            )
            transaction.set_rollback(True)
            return False

    return True


def process_items(items_to_process):
    """Process items and categorize them."""
    events_bulk = []
    anime_to_process = []

    for item in items_to_process:
        if item.media_type == MediaTypes.ANIME.value:
            anime_to_process.append(item)
        elif item.media_type == MediaTypes.TV.value:
            process_tv(item, events_bulk)
        elif item.media_type == MediaTypes.COMIC.value:
            process_comic(item, events_bulk)
        else:
            process_other(item, events_bulk)

    process_anime_bulk(anime_to_process, events_bulk)
    return events_bulk


def save_events(events_bulk):
    """Save events in bulk with proper conflict handling."""
    items_updated = set()

    existing_events = Event.objects.filter(
        item__in=[event.item for event in events_bulk],
    ).select_related("item")

    existing_with_content = {
        (event.item_id, event.content_number): event
        for event in existing_events
        if event.content_number is not None
    }
    existing_without_content = {
        event.item_id: event
        for event in existing_events
        if event.content_number is None
    }

    to_create = []
    to_update = []

    for event in events_bulk:
        items_updated.add(event.item)

        if event.content_number is not None:
            key = (event.item_id, event.content_number)
            if key in existing_with_content:
                existing_event = existing_with_content[key]
                existing_event.datetime = event.datetime
                to_update.append(existing_event)
            else:
                to_create.append(event)
        elif event.item_id in existing_without_content:
            existing_event = existing_without_content[event.item_id]
            existing_event.datetime = event.datetime
            to_update.append(existing_event)
        else:
            to_create.append(event)

    if to_create:
        Event.objects.bulk_create(to_create)

    if to_update:
        Event.objects.bulk_update(to_update, ["datetime"])

    logger.info(
        "Successfully processed %d events (%d created, %d updated)",
        len(events_bulk),
        len(to_create),
        len(to_update),
    )

    return items_updated


def format_items(items):
    """Format items as an indented list, one per line."""
    return "\n".join(f"  - {item} ({item.get_media_type_display()})" for item in items)


def generate_final_message(items_to_process, items_updated):
    """Generate the final message summarizing the results."""
    processed_details = format_items(items_to_process)

    if items_updated:
        success_details = format_items(items_updated)
        return (
            f"Processed {len(items_to_process)} items:\n{processed_details}\n\n"
            f"Releases updated for {len(items_updated)} items:\n{success_details}"
        )

    return (
        f"Processed {len(items_to_process)} items:\n{processed_details}\n\n"
        f"No releases have been updated."
    )


def cleanup_invalid_events(events_bulk):
    """Remove events that are no longer valid based on updated items."""
    processed_items = {}

    for event in events_bulk:
        if event.content_number is not None:
            processed_items.setdefault(event.item.id, set()).add(event.content_number)

    all_events = Event.objects.filter(
        item_id__in=processed_items.keys(),
    ).select_related("item")

    events_to_delete = []

    for event in all_events:
        if (
            event.content_number is not None
            and event.item_id in processed_items
            and event.content_number not in processed_items[event.item_id]
        ):
            logger.info(
                "Invalid event detected: %s - Number %s (scheduled for %s)",
                event.item,
                event.content_number,
                event.datetime,
            )
            events_to_delete.append(event.id)

    if events_to_delete:
        deleted_count = Event.objects.filter(id__in=events_to_delete).delete()[0]
        logger.info("Deleted %s invalid events for updated items", deleted_count)
