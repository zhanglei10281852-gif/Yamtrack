import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    """Add the per-entry generation marker for calendar snapshot commits."""

    dependencies = [
        ("app", "0064_restore_tz_shifted_media_dates"),
        ("events", "0016_fix_sentinel_datetimes"),
    ]

    operations = [
        migrations.CreateModel(
            name="CalendarRevision",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("generation", models.PositiveBigIntegerField(default=0)),
                ("committed_generation", models.PositiveBigIntegerField(default=0)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "item",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="calendar_revision",
                        to="app.item",
                    ),
                ),
            ],
        ),
    ]
