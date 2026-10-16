import datetime
import importlib
import json
import uuid
from collections import defaultdict
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from celery import states
from django.apps import apps as django_apps
from django.contrib.auth import get_user_model
from django.db.utils import OperationalError
from django.test import TestCase
from django_celery_beat.models import CrontabSchedule, PeriodicTask
from django_celery_results.models import TaskResult

from app.models import Anime, Item, MediaTypes, Sources, Status
from events.tasks import reload_calendar
from integrations import tasks
from integrations.imports import generations, helpers
from integrations.models import ImportPlanGeneration

migration = importlib.import_module(
    "integrations.migrations.0002_backfill_import_generations",
)


@contextmanager
def aware_schedule_times():
    """Make schedule time parsing tz-aware, independent of the host localtime.

    ``naive.astimezone()`` relies on the OS localtime database, which is broken
    in some sandboxes; aware datetimes convert purely with tzinfo data.
    """

    class AwareDateTime(datetime.datetime):
        @classmethod
        def strptime(cls, string, fmt):
            parsed = super().strptime(string, fmt)
            return parsed.replace(tzinfo=datetime.UTC)

    with patch("integrations.imports.helpers.datetime") as mock_datetime:
        mock_datetime.datetime = AwareDateTime
        mock_datetime.timedelta = datetime.timedelta
        yield


def scheduled_kwargs(user, generation_id=None):
    """Build the kwargs blob of a periodic import message."""
    kwargs = {"username": "testuser", "user_id": user.id, "mode": "new"}
    if generation_id is not None:
        kwargs[generations.PLAN_GENERATION_KWARG] = str(generation_id)
    return kwargs


class ImportGenerationTests(TestCase):
    """Tests for plan generations from creation through termination."""

    def setUp(self):
        """Create user and crontab."""
        self.credentials = {"username": "test", "password": "12345"}
        self.user = get_user_model().objects.create_user(**self.credentials)
        self.crontab = CrontabSchedule.objects.create(
            minute="0",
            hour="0",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        )

    def _make_periodic_task(
        self,
        name,
        generation_id=None,
        task="Import from MyAnimeList",
    ):
        kwargs = scheduled_kwargs(self.user, generation_id)
        return PeriodicTask.objects.create(
            name=name,
            task=task,
            kwargs=json.dumps(kwargs),
            crontab=self.crontab,
            enabled=True,
        )

    def _make_generation(self, name="Import from MyAnimeList for testuser at daily"):
        return ImportPlanGeneration.objects.create(
            user=self.user,
            task_name="Import from MyAnimeList",
            periodic_task_name=name,
        )

    def test_create_schedule_mints_active_generation(self):
        """Every new schedule carries a new active generation in kwargs."""
        request = MagicMock()
        request.user = self.user

        with aware_schedule_times():
            helpers.create_import_schedule(
                "testuser",
                request,
                "new",
                "daily",
                "14:30",
                "MyAnimeList",
            )

        periodic = PeriodicTask.objects.get(
            name="Import from MyAnimeList for testuser at 14:30:00 daily",
        )
        kwargs = json.loads(periodic.kwargs)
        generation = ImportPlanGeneration.objects.get(
            generation_id=kwargs["plan_generation"],
        )

        self.assertTrue(generation.is_active)
        self.assertEqual(generation.user, self.user)
        self.assertEqual(generation.task_name, "Import from MyAnimeList")
        self.assertEqual(
            json.loads(periodic.headers),
            {"plan_generation": str(generation.pk)},
        )

    def test_rebuilding_same_name_mints_new_generation(self):
        """A recreated plan gets a distinct generation; the old one stays closed."""
        request = MagicMock()
        request.user = self.user
        name = "Import from MyAnimeList for testuser at 14:30:00 daily"

        with aware_schedule_times():
            helpers.create_import_schedule(
                "testuser", request, "new", "daily", "14:30", "MyAnimeList"
            )
        first = PeriodicTask.objects.get(name=name)
        first_generation = json.loads(first.kwargs)["plan_generation"]
        generations.close_generation(first)

        with aware_schedule_times():
            helpers.create_import_schedule(
                "testuser", request, "new", "daily", "14:30", "MyAnimeList"
            )
        rebuilt = PeriodicTask.objects.get(name=name)
        rebuilt_generation = json.loads(rebuilt.kwargs)["plan_generation"]

        self.assertNotEqual(first_generation, rebuilt_generation)
        self.assertFalse(
            ImportPlanGeneration.objects.get(
                generation_id=first_generation,
            ).is_active,
        )
        self.assertTrue(
            ImportPlanGeneration.objects.get(
                generation_id=rebuilt_generation,
            ).is_active,
        )

    @patch("integrations.imports.mal.importer")
    def test_active_generation_and_one_time_import_run_normally(self, mock_importer):
        """Open plans and generation-less one-time imports are unaffected."""
        mock_importer.return_value = ({"anime": 1}, "")
        generation = self._make_generation()

        result = tasks.import_mal.apply_async(
            kwargs=scheduled_kwargs(self.user, generation.pk),
        )

        self.assertEqual(result.state, states.SUCCESS)
        self.assertEqual(mock_importer.call_count, 1)
        reload_calendar.delay.assert_called_once_with()

        # One-time imports have no generation at all.
        mock_importer.reset_mock()
        reload_calendar.delay.reset_mock()
        one_time_kwargs = {
            "username": "testuser",
            "user_id": self.user.id,
            "mode": "new",
        }
        result = tasks.import_mal.apply_async(kwargs=one_time_kwargs)
        self.assertEqual(result.state, states.SUCCESS)
        self.assertEqual(mock_importer.call_count, 1)
        reload_calendar.delay.assert_called_once_with()

    @patch("integrations.imports.mal.importer")
    def test_closed_generation_terminates_before_any_write(self, mock_importer):
        """A queued message of a deleted plan terminates without side effects."""
        generation = self._make_generation()
        generation.close()
        reload_calendar.delay.reset_mock()

        result = tasks.import_mal.apply_async(
            kwargs=scheduled_kwargs(self.user, generation.pk),
        )

        self.assertEqual(result.state, states.IGNORED)
        mock_importer.assert_not_called()
        reload_calendar.delay.assert_not_called()
        task_result = TaskResult.objects.get(task_id=result.id)
        self.assertEqual(task_result.status, states.REVOKED)
        self.assertIn("schedule was deleted", json.loads(task_result.result))

    @patch("integrations.imports.mal.importer")
    def test_unknown_generation_terminates(self, mock_importer):
        """Messages pointing at no existing generation terminate as well."""
        result = tasks.import_mal.apply_async(
            kwargs=scheduled_kwargs(self.user, uuid.uuid4()),
        )

        self.assertEqual(result.state, states.IGNORED)
        mock_importer.assert_not_called()
        self.assertEqual(
            TaskResult.objects.get(task_id=result.id).status,
            states.REVOKED,
        )

    def test_write_gate_rejects_closed_generation_before_first_write(self):
        """The write zone is not entered at all for a closed generation."""
        generation = self._make_generation()
        generation.close()
        token = generations.bind_current_generation(str(generation.pk))
        try:
            with (
                patch("integrations.imports.helpers.cleanup_existing_media") as cleanup,
                patch("integrations.imports.helpers.bulk_create_media") as create,
                self.assertRaises(generations.ImportGenerationClosedError),
            ):
                helpers.commit_import_changes(
                    self.user,
                    to_delete=defaultdict(lambda: defaultdict(set)),
                    bulk_media=defaultdict(list),
                )
        finally:
            generations.reset_current_generation(token)

        cleanup.assert_not_called()
        create.assert_not_called()

    def test_write_zone_rolls_back_completely_on_failure(self):
        """A failure mid-write rolls deletes and creates back together."""
        generation = self._make_generation()
        item = Item.objects.create(
            media_id="99",
            source=Sources.MAL.value,
            media_type=MediaTypes.ANIME.value,
            title="Rollback Show",
        )
        anime = Anime.objects.create(
            item=item,
            user=self.user,
            status=Status.PLANNING.value,
        )

        to_delete = defaultdict(lambda: defaultdict(set))
        to_delete[MediaTypes.ANIME.value][Sources.MAL.value].add("99")

        token = generations.bind_current_generation(str(generation.pk))
        try:
            with (
                patch(
                    "integrations.imports.helpers.bulk_create_media",
                    side_effect=RuntimeError("write failed"),
                ),
                self.assertRaises(RuntimeError),
            ):
                helpers.commit_import_changes(
                    self.user,
                    to_delete=to_delete,
                    bulk_media=defaultdict(list),
                )
        finally:
            generations.reset_current_generation(token)

        # The delete that happened first must have been rolled back: no half import.
        self.assertTrue(Anime.objects.filter(pk=anime.pk).exists())

    def test_close_generation_and_revoke_pending_runs(self):
        """Closing flips status, removes the schedule and revokes pending runs."""
        generation = self._make_generation()
        periodic = self._make_periodic_task(
            generation.periodic_task_name,
            generation.pk,
        )
        TaskResult.objects.create(
            task_id="pending-old",
            task_name="Import from MyAnimeList",
            task_kwargs=(
                f"{{'plan_generation': '{generation.pk}', 'user_id': {self.user.id}}}"
            ),
            status=states.PENDING,
        )
        TaskResult.objects.create(
            task_id="pending-other",
            task_name="Import from MyAnimeList",
            task_kwargs=f"{{'plan_generation': '{uuid.uuid4()}'}}",
            status=states.PENDING,
        )
        TaskResult.objects.create(
            task_id="started-old",
            task_name="Import from MyAnimeList",
            task_kwargs=(
                f"{{'plan_generation': '{generation.pk}', "
                f"'user_id': {self.user.id}}}"
            ),
            status=states.STARTED,
        )

        generation_id = generations.close_generation(periodic)

        self.assertEqual(generation_id, str(generation.pk))
        self.assertFalse(PeriodicTask.objects.filter(pk=periodic.pk).exists())
        generation.refresh_from_db()
        self.assertEqual(generation.status, ImportPlanGeneration.Status.CLOSED)
        self.assertIsNotNone(generation.closed_at)

        generations.revoke_pending_runs(generation_id)
        self.assertEqual(
            TaskResult.objects.get(task_id="pending-old").status,
            states.REVOKED,
        )
        self.assertEqual(
            TaskResult.objects.get(task_id="started-old").status,
            states.REVOKED,
        )
        self.assertEqual(
            TaskResult.objects.get(task_id="pending-other").status,
            states.PENDING,
        )

    def test_close_generation_fails_while_write_owns_generation(self):
        """A locked generation aborts the close and leaves the plan active."""
        generation = self._make_generation()
        periodic = self._make_periodic_task(
            generation.periodic_task_name,
            generation.pk,
        )

        with (
            patch(
                "integrations.imports.generations.connection.features."
                "has_select_for_update_nowait",
                new=True,
            ),
            patch(
                "integrations.models.ImportPlanGeneration.objects"
                ".select_for_update",
                side_effect=OperationalError("could not obtain lock"),
            ),
            self.assertRaises(generations.PlanWriteInProgressError),
        ):
            generations.close_generation(periodic)

        self.assertTrue(PeriodicTask.objects.filter(pk=periodic.pk).exists())
        generation.refresh_from_db()
        self.assertTrue(generation.is_active)

    def test_backfill_gives_legacy_schedules_a_generation(self):
        """The data migration upgrades existing scheduler rows in place."""
        legacy = self._make_periodic_task(
            "Import from Trakt for legacy at 00:00:00 daily",
            generation_id=None,
            task="Import from Trakt",
        )
        PeriodicTask.objects.create(
            name="Not an import",
            task="Reload calendar",
            kwargs=json.dumps({"user_id": self.user.id}),
            crontab=self.crontab,
        )

        migration.backfill_import_generations(django_apps, None)

        legacy.refresh_from_db()
        kwargs = json.loads(legacy.kwargs)
        self.assertIn("plan_generation", kwargs)
        generation = ImportPlanGeneration.objects.get(
            generation_id=kwargs["plan_generation"],
        )
        self.assertTrue(generation.is_active)
        self.assertEqual(generation.task_name, "Import from Trakt")
