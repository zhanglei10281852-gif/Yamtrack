import json

from django.db import migrations


def backfill_import_generations(apps, schema_editor):
    """Give every existing periodic import plan its own active generation.

    The generation id is injected into the task kwargs so messages Beat already
    published under the old schema remain distinguishable once the plan is
    deleted and later recreated with the same name.
    """
    ImportPlanGeneration = apps.get_model("integrations", "ImportPlanGeneration")
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")
    User = apps.get_model("users", "User")

    periodic_tasks = PeriodicTask.objects.filter(task__startswith="Import from ")
    for periodic_task in periodic_tasks:
        try:
            kwargs = json.loads(periodic_task.kwargs or "{}")
        except json.JSONDecodeError:
            continue

        if not isinstance(kwargs, dict) or kwargs.get("plan_generation"):
            continue

        user = User.objects.filter(id=kwargs.get("user_id")).first()
        if user is None:
            continue

        generation = ImportPlanGeneration.objects.create(
            task_name=periodic_task.task,
            periodic_task_name=periodic_task.name,
            user=user,
        )
        kwargs["plan_generation"] = str(generation.pk)
        periodic_task.kwargs = json.dumps(kwargs)
        periodic_task.save(update_fields=["kwargs"])


class Migration(migrations.Migration):
    """Backfill generations for existing periodic import schedules."""

    dependencies = [
        ("integrations", "0001_importplangeneration"),
        ("django_celery_beat", "0019_alter_periodictasks_options"),
        ("users", "0056_user_home_hide_unreleased"),
    ]

    operations = [
        migrations.RunPython(
            backfill_import_generations,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
