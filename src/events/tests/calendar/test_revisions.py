from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from events.calendar.main import (
    commit_entry_snapshot,
    commit_snapshots,
    fetch_releases,
    group_events_by_entry,
)
from events.calendar.revisions import reserve_generation
from events.models import CalendarRevision, Event
from events.tests.calendar.utils import CalendarFixturesMixin


def make_event(item, content_number, dt):
    """Build an unsaved event representing a fetched release snapshot."""
    return Event(item=item, content_number=content_number, datetime=dt)


class CalendarRevisionTests(CalendarFixturesMixin, TestCase):
    """Generation reservation and complete-snapshot commit semantics."""

    def test_reserve_generation_is_monotonic_and_per_entry(self):
        """Generations increase per item and are independent between items."""
        self.assertEqual(reserve_generation(self.tv_item), 1)
        self.assertEqual(reserve_generation(self.tv_item), 2)
        self.assertEqual(reserve_generation(self.movie_item), 1)
        self.assertEqual(reserve_generation(self.tv_item), 3)

    def test_latest_round_updates_dates_and_deletes_canceled_episodes(self):
        """A non-overlapping refresh keeps the historical calendar behavior."""
        generation = reserve_generation(self.tv_item)
        now = timezone.now()

        old_date = now + timedelta(days=5)
        canceled_date = now + timedelta(days=6)
        new_date = now + timedelta(days=7)
        new_episode_date = now + timedelta(days=8)

        Event.objects.create(
            item=self.season_item,
            content_number=1,
            datetime=old_date,
        )
        Event.objects.create(
            item=self.season_item,
            content_number=2,
            datetime=canceled_date,
        )

        committed = commit_entry_snapshot(
            self.tv_item,
            [
                make_event(self.season_item, 1, new_date),
                make_event(self.season_item, 3, new_episode_date),
            ],
            generation,
        )

        self.assertTrue(committed)
        remaining = dict(
            Event.objects.filter(item=self.season_item).values_list(
                "content_number",
                "datetime",
            ),
        )
        self.assertEqual(set(remaining), {1, 3})
        self.assertEqual(remaining[1], new_date)

        revision = CalendarRevision.objects.get(item=self.tv_item)
        self.assertEqual(revision.committed_generation, generation)

    def test_stale_round_does_not_overwrite_or_delete_newer_snapshot(self):
        """An earlier round finishing later must yield to the newer round.

        Reproduces the import-then-manual-refresh race: round A starts first,
        round B (manual refresh) starts later and commits newly published
        episodes; A's late snapshot must neither overwrite B's dates nor delete
        B's events.
        """
        generation_a = reserve_generation(self.tv_item)
        generation_b = reserve_generation(self.tv_item)

        date_a = timezone.now() + timedelta(days=10)
        date_b = timezone.now() + timedelta(days=30)

        self.assertTrue(
            commit_entry_snapshot(
                self.tv_item,
                [
                    make_event(self.season_item, 1, date_b),
                    make_event(self.season_item, 2, date_b),
                ],
                generation_b,
            ),
        )

        self.assertFalse(
            commit_entry_snapshot(
                self.tv_item,
                [make_event(self.season_item, 1, date_a)],
                generation_a,
            ),
        )

        events = {
            event.content_number: event.datetime
            for event in Event.objects.filter(item=self.season_item)
        }
        self.assertEqual(set(events), {1, 2})
        self.assertEqual(events[1], date_b)

        revision = CalendarRevision.objects.get(item=self.tv_item)
        self.assertEqual(revision.generation, generation_b)
        self.assertEqual(revision.committed_generation, generation_b)

    def test_failed_round_keeps_previous_calendar_and_later_round_takes_over(self):
        """A round that never commits preserves the previous full snapshot."""
        generation_one = reserve_generation(self.tv_item)
        date_one = timezone.now() + timedelta(days=1)
        self.assertTrue(
            commit_entry_snapshot(
                self.tv_item,
                [
                    make_event(self.season_item, 1, date_one),
                    make_event(self.season_item, 2, date_one),
                ],
                generation_one,
            ),
        )

        # A newer round registers but fails during fetching, so it never
        # attempts a commit.
        generation_failed = reserve_generation(self.tv_item)
        self.assertEqual(
            commit_snapshots([self.tv_item], [], {self.tv_item.id: generation_failed}),
            set(),
        )

        events = Event.objects.filter(item=self.season_item)
        self.assertEqual(set(events.values_list("content_number", flat=True)), {1, 2})

        revision = CalendarRevision.objects.get(item=self.tv_item)
        self.assertEqual(revision.generation, generation_failed)
        self.assertEqual(revision.committed_generation, generation_one)

        # A subsequent round can still take over and extend the calendar.
        generation_three = reserve_generation(self.tv_item)
        date_three = timezone.now() + timedelta(days=3)
        self.assertTrue(
            commit_entry_snapshot(
                self.tv_item,
                [
                    make_event(self.season_item, 1, date_one),
                    make_event(self.season_item, 2, date_one),
                    make_event(self.season_item, 3, date_three),
                ],
                generation_three,
            ),
        )
        self.assertTrue(
            Event.objects.filter(item=self.season_item, content_number=3).exists(),
        )
        self.assertEqual(
            CalendarRevision.objects.get(item=self.tv_item).committed_generation,
            generation_three,
        )

    def test_failed_commit_rolls_back_entire_snapshot(self):
        """Errors during the commit transaction must not leave half a snapshot."""
        generation = reserve_generation(self.movie_item)
        old_date = timezone.now() - timedelta(days=2)
        new_date = timezone.now() + timedelta(days=2)

        Event.objects.create(
            item=self.movie_item,
            content_number=None,
            datetime=old_date,
        )

        with (
            patch(
                "events.calendar.main.cleanup_invalid_events",
                side_effect=RuntimeError("cleanup failed"),
            ),
            self.assertRaises(RuntimeError),
        ):
            commit_entry_snapshot(
                self.movie_item,
                [make_event(self.movie_item, None, new_date)],
                generation,
            )

        self.assertEqual(
            Event.objects.get(item=self.movie_item).datetime,
            old_date,
        )
        self.assertEqual(
            CalendarRevision.objects.get(item=self.movie_item).committed_generation,
            0,
        )

    def test_stale_entry_skipped_without_blocking_other_entries(self):
        """Commit rights are evaluated independently per media entry."""
        movie_generation_a = reserve_generation(self.movie_item)
        book_generation = reserve_generation(self.book_item)
        # A newer round registered for the movie only.
        movie_generation_b = reserve_generation(self.movie_item)

        movie_date = timezone.now() + timedelta(days=4)
        book_date = timezone.now() + timedelta(days=5)

        items_updated = commit_snapshots(
            [self.movie_item, self.book_item],
            [
                make_event(self.movie_item, None, movie_date),
                make_event(self.book_item, 328, book_date),
            ],
            {
                self.movie_item.id: movie_generation_a,
                self.book_item.id: book_generation,
            },
        )

        self.assertFalse(Event.objects.filter(item=self.movie_item).exists())
        self.assertTrue(Event.objects.filter(item=self.book_item).exists())
        self.assertEqual(items_updated, {self.book_item})
        self.assertEqual(
            CalendarRevision.objects.get(item=self.movie_item).committed_generation,
            0,
        )
        self.assertEqual(
            CalendarRevision.objects.get(item=self.book_item).committed_generation,
            book_generation,
        )
        # The newer movie round can still commit afterwards.
        self.assertTrue(
            commit_entry_snapshot(
                self.movie_item,
                [make_event(self.movie_item, None, movie_date)],
                movie_generation_b,
            ),
        )

    def test_group_events_attributes_season_events_to_tv_entry(self):
        """Season events belong to the snapshot of their parent TV entry."""
        now = timezone.now()
        grouped = group_events_by_entry(
            [self.tv_item, self.movie_item],
            [
                make_event(self.season_item, 1, now),
                make_event(self.movie_item, None, now),
            ],
        )

        self.assertEqual(set(grouped), {self.tv_item.id, self.movie_item.id})
        self.assertEqual(
            [event.item for event in grouped[self.tv_item.id]],
            [self.season_item],
        )

    @patch("events.calendar.main.process_tv")
    def test_fetch_releases_rounds_and_late_stale_commit(self, mock_process_tv):
        """End-to-end: later refresh wins, late older round cannot delete."""
        date_one = timezone.now() + timedelta(days=11)
        date_two = timezone.now() + timedelta(days=22)

        mock_process_tv.side_effect = lambda _, events_bulk: events_bulk.append(
            make_event(self.season_item, 1, date_one),
        )
        fetch_releases(items_to_process=[self.tv_item])

        revision = CalendarRevision.objects.get(item=self.tv_item)
        self.assertEqual(revision.generation, 1)
        self.assertEqual(revision.committed_generation, 1)

        mock_process_tv.side_effect = lambda _, events_bulk: [
            events_bulk.append(make_event(self.season_item, 1, date_two)),
            events_bulk.append(make_event(self.season_item, 2, date_two)),
        ]
        fetch_releases(items_to_process=[self.tv_item])

        events = {
            event.content_number: event.datetime
            for event in Event.objects.filter(item=self.season_item)
        }
        self.assertEqual(events[1], date_two)
        self.assertIn(2, events)

        # The first round's result is committed late and must be rejected.
        self.assertFalse(
            commit_entry_snapshot(
                self.tv_item,
                [make_event(self.season_item, 1, date_one)],
                1,
            ),
        )
        events = {
            event.content_number: event.datetime
            for event in Event.objects.filter(item=self.season_item)
        }
        self.assertEqual(events[1], date_two)
        self.assertIn(2, events)

    @patch("events.calendar.main.process_other")
    def test_fetch_releases_without_events_keeps_previous_snapshot(
        self,
        mock_process_other,
    ):
        """An unsuccessful fetch round leaves existing events untouched."""
        old_date = timezone.now() - timedelta(days=1)
        Event.objects.create(
            item=self.movie_item,
            content_number=None,
            datetime=old_date,
        )

        mock_process_other.side_effect = lambda *_: None
        fetch_releases(items_to_process=[self.movie_item])

        self.assertEqual(Event.objects.get(item=self.movie_item).datetime, old_date)
        revision = CalendarRevision.objects.get(item=self.movie_item)
        self.assertEqual(revision.generation, 1)
        self.assertEqual(revision.committed_generation, 0)

        # The next successful round can still commit.
        new_date = timezone.now() + timedelta(days=9)
        mock_process_other.side_effect = lambda item, events_bulk: events_bulk.append(
            make_event(item, None, new_date),
        )
        fetch_releases(items_to_process=[self.movie_item])

        self.assertEqual(Event.objects.get(item=self.movie_item).datetime, new_date)
        self.assertEqual(
            CalendarRevision.objects.get(item=self.movie_item).committed_generation,
            2,
        )
