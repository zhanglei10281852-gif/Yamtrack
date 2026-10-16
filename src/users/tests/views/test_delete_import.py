import datetime
import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from celery import states
from django.contrib.auth import get_user_model
from django.contrib.messages import get_messages
from django.test import TestCase
from django.urls import reverse
from django_celery_beat.models import CrontabSchedule, PeriodicTask
from django_celery_results.models import TaskResult

from events.tasks import reload_calendar
from integrations import tasks
from integrations.imports import generations, helpers
from integrations.models import ImportPlanGeneration


@contextmanager
def aware_schedule_times():
    """Make schedule time parsing independent of the host localtime database."""

    class AwareDateTime(datetime.datetime):
        @classmethod
        def strptime(cls, string, fmt):
            parsed = super().strptime(string, fmt)
            return parsed.replace(tzinfo=datetime.UTC)

    with patch("integrations.imports.helpers.datetime") as mock_datetime:
        mock_datetime.datetime = AwareDateTime
        mock_datetime.timedelta = datetime.timedelta
        yield


class DeleteImportScheduleTests(TestCase):
    """Tests for the delete_import_schedule view."""

    def setUp(self):
        """Create user and test data for the tests."""
        self.credentials = {"username": "testuser", "password": "testpass123"}
        self.user = get_user_model().objects.create_user(**self.credentials)
        self.client.login(**self.credentials)

        self.crontab = CrontabSchedule.objects.create(
            minute="0",
            hour="0",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        )

        self.task = PeriodicTask.objects.create(
            name="Import from Trakt for testuser at daily",
            task="Import from Trakt",
            kwargs=f'{{"user_id": {self.user.id}, "username": "testuser"}}',
            crontab=self.crontab,
            enabled=True,
        )

        self.other_credentials = {"username": "otheruser", "password": "testpass123"}
        self.other_user = get_user_model().objects.create_user(**self.other_credentials)

        self.other_task = PeriodicTask.objects.create(
            name="Import from Trakt for otheruser at daily",
            task="Import from Trakt",
            kwargs=f'{{"user_id": {self.other_user.id}, "username": "otheruser"}}',
            crontab=self.crontab,
            enabled=True,
        )

    def test_delete_import_schedule_success(self):
        """Test successful deletion of an import schedule."""
        response = self.client.post(
            reverse("delete_import_schedule"),
            {
                "task_name": self.task.name,
            },
        )
        self.assertRedirects(response, reverse("import_data"))

        with self.assertRaises(PeriodicTask.DoesNotExist):
            PeriodicTask.objects.get(id=self.task.id)

        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(len(messages), 1)
        self.assertIn("Import schedule deleted", str(messages[0]))

        self.assertTrue(PeriodicTask.objects.filter(id=self.other_task.id).exists())

    def test_delete_import_schedule_not_found(self):
        """Test deletion of a non-existent import schedule."""
        response = self.client.post(
            reverse("delete_import_schedule"),
            {
                "task_name": "Non-existent Task",
            },
        )
        self.assertRedirects(response, reverse("import_data"))

        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(len(messages), 1)
        self.assertIn("Import schedule not found", str(messages[0]))

        self.assertTrue(PeriodicTask.objects.filter(id=self.task.id).exists())

    def test_delete_import_schedule_other_user(self):
        """Test deletion of another user's import schedule."""
        response = self.client.post(
            reverse("delete_import_schedule"),
            {
                "task_name": self.other_task.name,
            },
        )
        self.assertRedirects(response, reverse("import_data"))

        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(len(messages), 1)
        self.assertIn("Import schedule not found", str(messages[0]))

        self.assertTrue(PeriodicTask.objects.filter(id=self.other_task.id).exists())


class DeleteImportScheduleGenerationTests(TestCase):
    """Tests for generation-aware deletion of import schedules."""

    def setUp(self):
        """Create user and a real periodic plan with its generation."""
        self.credentials = {"username": "testuser", "password": "testpass123"}
        self.user = get_user_model().objects.create_user(**self.credentials)
        self.client.login(**self.credentials)

        self.crontab = CrontabSchedule.objects.create(
            minute="0",
            hour="0",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        )

        request = MagicMock()
        request.user = self.user
        with aware_schedule_times():
            helpers.create_import_schedule(
                "testuser",
                request,
                "new",
                "daily",
                "00:00",
                "MyAnimeList",
            )
        self.plan_name = "Import from MyAnimeList for testuser at 00:00:00 daily"
        self.periodic_task = PeriodicTask.objects.get(name=self.plan_name)
        self.old_generation = json.loads(self.periodic_task.kwargs)["plan_generation"]

    def _delete_plan(self):
        return self.client.post(
            reverse("delete_import_schedule"),
            {"task_name": self.plan_name},
        )

    def test_delete_closes_generation_and_removes_schedule(self):
        """Successful deletion closes the generation in the same request."""
        response = self._delete_plan()
        self.assertRedirects(response, reverse("import_data"))

        self.assertFalse(
            PeriodicTask.objects.filter(pk=self.periodic_task.pk).exists(),
        )
        generation = ImportPlanGeneration.objects.get(
            generation_id=self.old_generation,
        )
        self.assertEqual(generation.status, ImportPlanGeneration.Status.CLOSED)
        self.assertIsNotNone(generation.closed_at)

        messages = list(get_messages(response.wsgi_request))
        self.assertIn("Import schedule deleted", str(messages[0]))

    def test_delete_revokes_pending_results_of_the_generation(self):
        """Runs already queued for the plan are marked stopped on deletion."""
        TaskResult.objects.create(
            task_id="queued-run",
            task_name="Import from MyAnimeList",
            task_kwargs=(
                f"{{'plan_generation': '{self.old_generation}', "
                f"'user_id': {self.user.id}}}"
            ),
            status=states.PENDING,
        )

        self._delete_plan()

        self.assertEqual(
            TaskResult.objects.get(task_id="queued-run").status,
            states.REVOKED,
        )

    @patch("integrations.imports.mal.importer")
    def test_old_message_after_delete_does_not_write_or_reload(self, mock_importer):
        """A message lingering in the queue terminates once the plan is gone."""
        self._delete_plan()
        reload_calendar.delay.reset_mock()

        result = tasks.import_mal.apply_async(
            kwargs={
                "username": "testuser",
                "user_id": self.user.id,
                "mode": "new",
                "plan_generation": self.old_generation,
            },
        )

        self.assertEqual(result.state, states.IGNORED)
        mock_importer.assert_not_called()
        reload_calendar.delay.assert_not_called()
        self.assertEqual(
            TaskResult.objects.get(task_id=result.id).status,
            states.REVOKED,
        )

    @patch("integrations.imports.mal.importer")
    def test_recreated_plan_only_accepts_new_generation(self, mock_importer):
        """Rebuilding the same name starts a fresh generation."""
        mock_importer.return_value = ({"anime": 2}, "")
        self._delete_plan()

        request = MagicMock()
        request.user = self.user
        with aware_schedule_times():
            helpers.create_import_schedule(
                "testuser",
                request,
                "new",
                "daily",
                "00:00",
                "MyAnimeList",
            )
        new_task = PeriodicTask.objects.get(name=self.plan_name)
        new_generation = json.loads(new_task.kwargs)["plan_generation"]
        self.assertNotEqual(self.old_generation, new_generation)

        # Old messages still terminate, new generation messages run and reload.
        reload_calendar.delay.reset_mock()
        old_result = tasks.import_mal.apply_async(
            kwargs={
                "username": "testuser",
                "user_id": self.user.id,
                "mode": "new",
                "plan_generation": self.old_generation,
            },
        )
        self.assertEqual(old_result.state, states.IGNORED)
        mock_importer.assert_not_called()

        new_result = tasks.import_mal.apply_async(
            kwargs={
                "username": "testuser",
                "user_id": self.user.id,
                "mode": "new",
                "plan_generation": new_generation,
            },
        )
        self.assertEqual(new_result.state, states.SUCCESS)
        self.assertEqual(mock_importer.call_count, 1)
        reload_calendar.delay.assert_called_once_with()

    def test_failed_close_keeps_the_plan_running(self):
        """If the close transaction fails, the plan and generation stay active."""
        with patch(
            "integrations.imports.generations.close_generation",
            side_effect=generations.PlanWriteInProgressError,
        ):
            response = self._delete_plan()

        self.assertRedirects(response, reverse("import_data"))
        self.assertTrue(
            PeriodicTask.objects.filter(pk=self.periodic_task.pk).exists(),
        )
        generation = ImportPlanGeneration.objects.get(
            generation_id=self.old_generation,
        )
        self.assertTrue(generation.is_active)

        messages = list(get_messages(response.wsgi_request))
        self.assertIn("currently writing", str(messages[0]))
