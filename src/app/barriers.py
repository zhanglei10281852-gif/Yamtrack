"""Causal barriers between explicit media deletions and external webhook writes.

Explicit user deletions (movies, TV shows/seasons and anime) and playback
events accepted from Plex, Jellyfin or Emby webhooks can race: an event that
was authenticated and accepted before the deletion may only finish its
metadata lookup afterwards and would otherwise recreate the just deleted
media graph.

This module establishes a comparable causal boundary using two primitives:

* :class:`app.models.MediaDeletionBarrier` rows are append-only generation
  markers. ``id`` is a globally increasing generation; a row is inserted in
  the same transaction that removes the media graph, so a committed barrier is
  equivalent to a committed deletion (and a rolled back deletion leaves no
  barrier behind).
* Webhook requests snapshot the newest generation visible to their user when
  they are accepted (``current_generation``). Immediately before creating or
  updating Movie, TV, Season, Episode or watch history rows, they enter
  :func:`webhook_write_gate`, which locks the user row (serializing against
  concurrent deletions) and re-checks that no barrier newer than the snapshot
  exists for the resolved target identity.

Requests accepted before a deletion commits lose their write qualification and
write nothing; requests accepted after the commit snapshot the new generation
and re-track normally. Manual re-adds never consult the barrier. Every write is
a single database transaction, so a restart can only leave either the old graph
or the deleted state, never a parent-only or episode-only partial graph.
"""

import logging
from contextlib import contextmanager

from django.db import transaction
from django.db.models import Max, Q

from app.models import MediaDeletionBarrier, MediaTypes
from users.models import User

logger = logging.getLogger(__name__)


def make_identity(
    source,
    media_id,
    media_type,
    season_number=None,
    episode_number=None,
):
    """Build a barrier identity matching the natural key of an Item."""
    return {
        "source": source,
        "media_id": str(media_id),
        "media_type": media_type,
        "season_number": season_number,
        "episode_number": episode_number,
    }


def current_generation(user):
    """Return the newest deletion generation visible for a user.

    Captured by webhooks right after authentication and event validation.
    Returns 0 when the user never deleted anything, which is older than every
    barrier row id.
    """
    return (
        MediaDeletionBarrier.objects.filter(user=user).aggregate(
            newest=Max("id"),
        )["newest"]
        or 0
    )


def _identities_filter(identities):
    q = Q()
    for identity in identities:
        q |= Q(**identity)
    return q


def _lock_user(user):
    """Lock the user row to serialize deletes against webhook writes.

    On PostgreSQL this is a row lock; SQLite serializes writers itself. The
    same lock order (user first) is used on both sides, so no deadlock is
    possible.
    """
    return User.objects.select_for_update().get(pk=user.pk)


def _media_owner(media):
    """Return the user owning a media instance (episodes are owned via season)."""
    owner = getattr(media, "user", None)
    if owner is None:
        owner = media.related_season.user
    return owner


def barrier_identities_for_media(media):
    """Return the identities shielded by deleting this media instance.

    * Deleting a TV show shields the show identity; every episode webhook
      write checks the show identity before touching seasons or episodes.
    * Deleting a season shields only that season, so events for other seasons
      of the same show keep working.
    * Deleting a single episode shields only that episode.
    * Every other media type shields its own identity.
    """
    item = media.item

    if item.media_type == MediaTypes.TV.value:
        return [
            make_identity(
                item.source,
                item.media_id,
                MediaTypes.TV.value,
            ),
        ]

    if item.media_type == MediaTypes.SEASON.value:
        return [
            make_identity(
                item.source,
                item.media_id,
                MediaTypes.SEASON.value,
                season_number=item.season_number,
            ),
        ]

    if item.media_type == MediaTypes.EPISODE.value:
        return [
            make_identity(
                item.source,
                item.media_id,
                MediaTypes.EPISODE.value,
                season_number=item.season_number,
                episode_number=item.episode_number,
            ),
        ]

    return [
        make_identity(
            item.source,
            item.media_id,
            item.media_type,
            season_number=item.season_number,
            episode_number=item.episode_number,
        ),
    ]


def record_explicit_deletion(media):
    """Bump deletion generations and remove the media graph atomically.

    The user row is locked before the target is read again, so the total order
    is deterministic: a webhook that starts its write phase while this deletion
    is in progress waits for this commit and then observes the barrier, while a
    webhook that committed earlier genuinely precedes the deletion.

    Either both the barrier rows and the cascade deletes commit, or nothing
    does, preserving the original semantics for the media and for requests
    still in processing.
    """
    user = _media_owner(media)
    with transaction.atomic():
        _lock_user(user)
        # Re-read under the lock so the cascade delete sees the committed
        # state at the causal boundary rather than a stale view instance.
        media = type(media).objects.select_for_update().get(pk=media.pk)
        MediaDeletionBarrier.objects.bulk_create(
            MediaDeletionBarrier(user=user, **identity)
            for identity in barrier_identities_for_media(media)
        )
        media.delete()
        logger.info(
            "Recorded explicit deletion barrier for %s (user %s)",
            media,
            user,
        )


@contextmanager
def webhook_write_gate(user, snapshot_generation, identities, media_label):
    """Re-confirm an accepted webhook event still owns its write qualification.

    All database writes for one media graph must happen inside the ``with``
    block. Handlers fetch their own metadata before entering; model save hooks
    may still consult metadata providers while the lock is held, which is safe
    because the user row is always the first and only lock taken.

    Yields ``False`` (without performing any write) when an explicit deletion
    newer than ``snapshot_generation`` targets one of ``identities``; yields
    ``True`` otherwise.
    """
    with transaction.atomic():
        _lock_user(user)

        has_newer_deletion = (
            MediaDeletionBarrier.objects.filter(user=user)
            .filter(
                _identities_filter(identities),
                id__gt=snapshot_generation,
            )
            .exists()
        )

        if has_newer_deletion:
            logger.info(
                "Discarding stale webhook write for %s (user %s): an explicit "
                "deletion newer than accepted generation %s was committed",
                media_label,
                user,
                snapshot_generation,
            )
            yield False
            return

        yield True
