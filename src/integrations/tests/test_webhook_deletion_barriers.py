"""Tests for the causal boundary between explicit deletions and webhooks.

An event authenticated and accepted before an explicit deletion must not
recreate the deleted media graph when its metadata lookup finishes later,
while genuinely new events and manual re-adds must keep working. All provider
calls are mocked so the tests stay deterministic and offline.
"""

import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from app import barriers
from app.models import (
    TV,
    Anime,
    Episode,
    Item,
    MediaDeletionBarrier,
    MediaTypes,
    Movie,
    Season,
    Sources,
    Status,
)
from integrations.webhooks.jellyfin import JellyfinWebhookProcessor

MOVIE_METADATA = {
    "title": "The Matrix",
    "image": "http://example.com/matrix.jpg",
    "max_progress": 1,
}

ANIME_METADATA = {
    "title": "Frieren",
    "image": "http://example.com/frieren.jpg",
    "max_progress": 12,
}

def _tv_with_seasons(_media_id, season_numbers):
    """Build tv_with_seasons metadata for any requested season."""
    number = season_numbers[0]
    return {
        "title": "Friends",
        "image": "http://example.com/friends.jpg",
        f"season/{number}": {
            "image": "http://example.com/season.jpg",
            "episodes": [
                {"episode_number": 1, "still_path": "/e1.jpg"},
                {"episode_number": 2, "still_path": None},
            ],
        },
    }


TV_SHOW_METADATA = {"related": {"seasons": []}}

PLAYED_PAYLOAD = {"Event": "MarkPlayed", "Item": {"UserData": {"Played": True}}}
UNPLAYED_PAYLOAD = {"Event": "MarkUnplayed", "Item": {"UserData": {"Played": False}}}


class WebhookDeletionBarrierTests(TestCase):
    """Causality tests for MediaDeletionBarrier and the webhook write gates."""

    def setUp(self):
        """Create a user and stub every provider call used by write paths."""
        self.client = Client()
        credentials = {
            "username": "testuser",
            "password": "password",
            "token": "test-token",
        }
        self.user = get_user_model().objects.create_user(**credentials)
        self.url = reverse("jellyfin_webhook", kwargs={"token": "test-token"})

        patches = [
            patch("app.providers.tmdb.movie", return_value=MOVIE_METADATA),
            patch("app.providers.mal.anime", return_value=ANIME_METADATA),
            patch(
                "app.providers.tmdb.tv_with_seasons",
                side_effect=_tv_with_seasons,
            ),
            patch("app.providers.tmdb.tv", return_value=TV_SHOW_METADATA),
            patch("app.models.Item.fetch_releases", lambda *_, **__: None),
        ]
        for started_patch in patches:
            started_patch.start()
            self.addCleanup(started_patch.stop)

    # -- helpers ---------------------------------------------------------

    def _processor(self):
        """Return a processor whose qualification snapshot is captured now."""
        processor = JellyfinWebhookProcessor()
        processor._snapshot_deletion_generation(self.user)
        return processor

    def _create_movie(self, media_id="603"):
        item, _ = Item.objects.get_or_create(
            media_id=media_id,
            source=Sources.TMDB.value,
            media_type=MediaTypes.MOVIE.value,
            defaults={
                "title": "The Matrix",
                "image": "http://example.com/matrix.jpg",
            },
        )
        return Movie.objects.create(
            item=item,
            user=self.user,
            progress=1,
            status=Status.COMPLETED.value,
        )

    def _create_tv_graph(self, media_id="1668", season_numbers=(1,)):
        tv_item = Item.objects.create(
            media_id=media_id,
            source=Sources.TMDB.value,
            media_type=MediaTypes.TV.value,
            title="Friends",
            image="http://example.com/friends.jpg",
        )
        tv = TV.objects.create(
            item=tv_item,
            user=self.user,
            status=Status.IN_PROGRESS.value,
        )
        seasons = {}
        for number in season_numbers:
            season_item = Item.objects.create(
                media_id=media_id,
                source=Sources.TMDB.value,
                media_type=MediaTypes.SEASON.value,
                title="Friends",
                image="http://example.com/season.jpg",
                season_number=number,
            )
            seasons[number] = Season.objects.create(
                item=season_item,
                user=self.user,
                related_tv=tv,
                status=Status.IN_PROGRESS.value,
            )
        return tv, seasons

    # -- generation snapshot ---------------------------------------------

    def test_generation_is_zero_without_deletions(self):
        """A fresh user has generation 0."""
        self.assertEqual(barriers.current_generation(self.user), 0)

    def test_snapshot_advances_after_deletion(self):
        """The accepted generation reflects committed deletions."""
        movie = self._create_movie()
        self.assertEqual(barriers.current_generation(self.user), 0)

        barriers.record_explicit_deletion(movie)

        self.assertEqual(barriers.current_generation(self.user), 1)

    # -- movie ------------------------------------------------------------

    def test_stale_movie_event_after_deletion_is_discarded(self):
        """An event accepted before deletion cannot recreate the movie."""
        movie = self._create_movie()
        stale_processor = self._processor()

        barriers.record_explicit_deletion(movie)
        history_count = Movie.history.count()

        stale_processor._handle_movie("603", PLAYED_PAYLOAD, self.user)

        self.assertFalse(
            Movie.objects.filter(
                item__media_id="603",
                user=self.user,
            ).exists(),
        )
        # The blocked attempt must not have written watch history either.
        self.assertEqual(Movie.history.count(), history_count)

    def test_new_movie_event_after_deletion_retracks(self):
        """A genuinely new event after deletion tracks the movie again."""
        movie = self._create_movie()
        barriers.record_explicit_deletion(movie)

        fresh_processor = self._processor()
        fresh_processor._handle_movie("603", PLAYED_PAYLOAD, self.user)

        recreated = Movie.objects.get(item__media_id="603", user=self.user)
        self.assertEqual(recreated.status, Status.COMPLETED.value)
        self.assertEqual(recreated.progress, 1)

    def test_manual_readd_not_blocked_by_old_deletion(self):
        """Manual re-add succeeds and a later new event updates it normally."""
        movie = self._create_movie()
        stale_processor = self._processor()
        barriers.record_explicit_deletion(movie)

        # The stale event is discarded.
        stale_processor._handle_movie("603", PLAYED_PAYLOAD, self.user)
        self.assertFalse(Movie.objects.filter(item__media_id="603").exists())

        # User manually re-adds the movie.
        manually_readded = self._create_movie()

        # A new event after the re-add writes normally (a scrobble on a
        # completed movie records a repeat instance).
        fresh_processor = self._processor()
        fresh_processor._handle_movie("603", PLAYED_PAYLOAD, self.user)

        self.assertEqual(
            Movie.objects.filter(item__media_id="603", user=self.user).count(),
            2,
        )
        # The manually re-added instance is never touched by the stale event.
        self.assertTrue(
            Movie.objects.filter(id=manually_readded.id).exists(),
        )

    def test_unrelated_deletion_does_not_block_other_movie(self):
        """Barriers are identity scoped: deleting one title spares another."""
        movie = self._create_movie("603")
        stale_processor = self._processor()
        barriers.record_explicit_deletion(movie)

        stale_processor._handle_movie("604", PLAYED_PAYLOAD, self.user)

        self.assertTrue(
            Movie.objects.filter(item__media_id="604", user=self.user).exists(),
        )

    def test_other_user_deletion_does_not_block_event(self):
        """Barriers never leak across users."""
        other_credentials = {"username": "other", "password": "password"}
        other_user = get_user_model().objects.create_user(**other_credentials)
        other_item = Item.objects.create(
            media_id="603",
            source=Sources.TMDB.value,
            media_type=MediaTypes.MOVIE.value,
            title="The Matrix",
            image="http://example.com/matrix.jpg",
        )
        other_movie = Movie.objects.create(
            item=other_item,
            user=other_user,
            status=Status.COMPLETED.value,
        )
        processor = self._processor()
        barriers.record_explicit_deletion(other_movie)

        processor._handle_movie("603", PLAYED_PAYLOAD, self.user)

        self.assertTrue(
            Movie.objects.filter(item__media_id="603", user=self.user).exists(),
        )

    # -- tv / season / episode graph --------------------------------------

    def test_stale_tv_event_leaves_no_partial_graph(self):
        """A stale TV event must not create parents or a lone episode."""
        tv, seasons = self._create_tv_graph("1668", (1,))
        season = seasons[1]
        episode_item = Item.objects.create(
            media_id="1668",
            source=Sources.TMDB.value,
            media_type=MediaTypes.EPISODE.value,
            title="Friends",
            image="http://example.com/e1.jpg",
            season_number=1,
            episode_number=1,
        )
        Episode.objects.create(
            item=episode_item,
            related_season=season,
            end_date=timezone.now(),
        )
        stale_processor = self._processor()

        barriers.record_explicit_deletion(tv)
        stale_processor._handle_tv_episode("1668", 1, 1, PLAYED_PAYLOAD, self.user)

        self.assertFalse(TV.objects.filter(item__media_id="1668").exists())
        self.assertFalse(
            Season.objects.filter(item__media_id="1668", item__season_number=1).exists()
        )
        self.assertFalse(
            Episode.objects.filter(
                item__media_id="1668",
                item__season_number=1,
                item__episode_number=1,
            ).exists()
        )

    def test_new_tv_event_recreates_full_graph(self):
        """A new event after deletion recreates TV, season and episode together."""
        tv, _ = self._create_tv_graph("1668", (1,))
        barriers.record_explicit_deletion(tv)

        fresh_processor = self._processor()
        fresh_processor._handle_tv_episode("1668", 1, 1, PLAYED_PAYLOAD, self.user)

        tv_instance = TV.objects.get(item__media_id="1668", user=self.user)
        season_instance = Season.objects.get(
            item__media_id="1668",
            item__season_number=1,
            user=self.user,
        )
        self.assertEqual(season_instance.related_tv, tv_instance)
        episode = Episode.objects.get(
            item__media_id="1668",
            item__season_number=1,
            item__episode_number=1,
        )
        self.assertEqual(episode.related_season, season_instance)
        self.assertIsNotNone(episode.end_date)

    def test_season_deletion_only_blocks_that_season(self):
        """Deleting one season must not block stale events for another season."""
        _, seasons = self._create_tv_graph("1668", (1, 2))
        stale_processor = self._processor()

        barriers.record_explicit_deletion(seasons[1])

        # Same old request, episode in the deleted season: blocked.
        stale_processor._handle_tv_episode("1668", 1, 1, PLAYED_PAYLOAD, self.user)
        self.assertFalse(
            Episode.objects.filter(item__season_number=1).exists(),
        )

        # Same old request, episode of the still tracked season: allowed.
        stale_processor._handle_tv_episode("1668", 2, 1, PLAYED_PAYLOAD, self.user)
        self.assertTrue(
            Episode.objects.filter(
                item__season_number=2,
                item__episode_number=1,
            ).exists(),
        )

    def test_single_episode_deletion_blocks_only_that_episode(self):
        """Deleting one episode must not block writes for other episodes."""
        _, seasons = self._create_tv_graph("1668", (1,))
        season = seasons[1]
        episode_item = Item.objects.create(
            media_id="1668",
            source=Sources.TMDB.value,
            media_type=MediaTypes.EPISODE.value,
            title="Friends",
            image="http://example.com/e1.jpg",
            season_number=1,
            episode_number=1,
        )
        episode = Episode.objects.create(
            item=episode_item,
            related_season=season,
            end_date=timezone.now(),
        )
        stale_processor = self._processor()

        barriers.record_explicit_deletion(episode)

        stale_processor._handle_tv_episode("1668", 1, 1, PLAYED_PAYLOAD, self.user)
        self.assertFalse(
            Episode.objects.filter(
                item__season_number=1,
                item__episode_number=1,
            ).exists(),
        )

        stale_processor._handle_tv_episode("1668", 1, 2, PLAYED_PAYLOAD, self.user)
        self.assertTrue(
            Episode.objects.filter(
                item__season_number=1,
                item__episode_number=2,
            ).exists(),
        )

    # -- anime ------------------------------------------------------------

    def test_stale_anime_event_blocked_and_new_event_retracks(self):
        """Anime writes obey the same generation boundary."""
        item = Item.objects.create(
            media_id="52991",
            source=Sources.MAL.value,
            media_type=MediaTypes.ANIME.value,
            title="Frieren",
            image="http://example.com/frieren.jpg",
        )
        anime = Anime.objects.create(
            item=item,
            user=self.user,
            progress=5,
            status=Status.IN_PROGRESS.value,
        )
        stale_processor = self._processor()

        barriers.record_explicit_deletion(anime)
        stale_processor._handle_anime("52991", 5, PLAYED_PAYLOAD, self.user)
        self.assertFalse(
            Anime.objects.filter(item__media_id="52991", user=self.user).exists(),
        )

        fresh_processor = self._processor()
        fresh_processor._handle_anime("52991", 5, PLAYED_PAYLOAD, self.user)
        self.assertTrue(
            Anime.objects.filter(item__media_id="52991", user=self.user).exists(),
        )

    # -- MarkUnplayed ------------------------------------------------------

    def test_stale_mark_unplayed_does_not_remove_retracked_movie(self):
        """An old unplayed event cannot delete media recreated after deletion."""
        movie = self._create_movie()
        stale_processor = self._processor()
        barriers.record_explicit_deletion(movie)

        # A new event re-tracked the movie after the deletion.
        self._processor()._handle_movie("603", PLAYED_PAYLOAD, self.user)

        stale_processor._handle_movie("603", UNPLAYED_PAYLOAD, self.user)
        self.assertTrue(
            Movie.objects.filter(item__media_id="603", user=self.user).exists(),
        )

    def test_new_mark_unplayed_still_deletes(self):
        """MarkUnplayed after a deletion keeps its normal behavior."""
        self._create_movie()

        fresh_processor = self._processor()
        fresh_processor._handle_movie("603", UNPLAYED_PAYLOAD, self.user)

        self.assertFalse(
            Movie.objects.filter(item__media_id="603", user=self.user).exists(),
        )

    def test_stale_mark_unplayed_tv_episode_is_blocked(self):
        """An old unplayed episode event cannot undo a later episode state."""
        _, seasons = self._create_tv_graph("1668", (1,))
        season = seasons[1]
        episode_item = Item.objects.create(
            media_id="1668",
            source=Sources.TMDB.value,
            media_type=MediaTypes.EPISODE.value,
            title="Friends",
            image="http://example.com/e1.jpg",
            season_number=1,
            episode_number=1,
        )
        Episode.objects.create(
            item=episode_item,
            related_season=season,
            end_date=timezone.now(),
        )
        stale_processor = self._processor()

        # User unwatched the episode explicitly (barrier), then a new event
        # watched it again.
        barriers.record_explicit_deletion(
            Episode.objects.get(item=episode_item),
        )
        self._processor()._handle_tv_episode(
            "1668",
            1,
            1,
            PLAYED_PAYLOAD,
            self.user,
        )

        stale_processor._handle_tv_episode(
            "1668",
            1,
            1,
            UNPLAYED_PAYLOAD,
            self.user,
        )
        self.assertTrue(
            Episode.objects.filter(
                item__season_number=1,
                item__episode_number=1,
            ).exists(),
        )

    def test_failed_tv_write_rolls_back_parents(self):
        """A failure after creating parents must not leave a partial graph."""
        processor = self._processor()
        with patch(
            "app.models.Episode.objects.create",
            side_effect=RuntimeError("write failed"),
        ), self.assertRaises(RuntimeError):
            processor._handle_tv_episode(
                "1668",
                1,
                1,
                PLAYED_PAYLOAD,
                self.user,
            )

        self.assertFalse(TV.objects.filter(item__media_id="1668").exists())
        self.assertFalse(Season.objects.filter(item__media_id="1668").exists())
        self.assertFalse(
            Episode.objects.filter(item__media_id="1668").exists(),
        )

    # -- duplicate suppression regression ----------------------------------

    def test_duplicate_episode_suppression_holds_through_gate(self):
        """Two near-simultaneous new events still create only one episode."""
        for _ in range(2):
            self._processor()._handle_tv_episode(
                "1668",
                1,
                1,
                PLAYED_PAYLOAD,
                self.user,
            )

        self.assertEqual(
            Episode.objects.filter(
                item__media_id="1668",
                item__season_number=1,
                item__episode_number=1,
            ).count(),
            1,
        )

    # -- delete failure and the HTTP view ----------------------------------

    def test_failed_delete_keeps_media_and_records_no_barrier(self):
        """A failing delete transaction rolls the barrier bump back too."""
        movie = self._create_movie()

        with (
            patch("app.models.Movie.delete", side_effect=RuntimeError("boom")),
            self.assertRaises(RuntimeError),
        ):
            barriers.record_explicit_deletion(movie)

        self.assertTrue(Movie.objects.filter(id=movie.id).exists())
        self.assertEqual(MediaDeletionBarrier.objects.count(), 0)

    def test_media_delete_view_records_barrier(self):
        """The explicit library delete endpoint commits barrier and removal."""
        movie = self._create_movie()
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("media_delete"),
            data={
                "instance_id": movie.id,
                "media_type": MediaTypes.MOVIE.value,
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(Movie.objects.filter(id=movie.id).exists())
        barrier = MediaDeletionBarrier.objects.get()
        self.assertEqual(barrier.user, self.user)
        self.assertEqual(barrier.source, Sources.TMDB.value)
        self.assertEqual(barrier.media_id, "603")
        self.assertEqual(barrier.media_type, MediaTypes.MOVIE.value)

    def test_new_event_through_http_retracks_after_delete(self):
        """End-to-end: snapshot is captured per request and new events work."""
        movie = self._create_movie()
        barriers.record_explicit_deletion(movie)

        self.user.jellyfin_mark_played_enabled = True
        self.user.save(update_fields=["jellyfin_mark_played_enabled"])

        payload = {
            "Event": "MarkPlayed",
            "Item": {
                "Type": "Movie",
                "Name": "The Matrix",
                "ProviderIds": {"Tmdb": "603"},
                "UserData": {"Played": True},
            },
        }
        response = self.client.post(
            self.url,
            data=json.dumps(payload),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            Movie.objects.filter(item__media_id="603", user=self.user).exists(),
        )
