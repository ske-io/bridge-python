"""``python manage.py ske_schedule``: the platform's scheduler command. It
sends the Celery beat entries due this minute and runs the scheduled tasks
(ske_bridge.schedule) due; see ske_bridge.schedule.command."""

from django.core.management.base import BaseCommand, CommandError

from ske_bridge.celery import find_app
from ske_bridge.schedule.command import run


class Command(BaseCommand):
    help = "Send the Celery beat schedule's entries and run the scheduled tasks that are due this minute."

    def handle(self, *args, **options):
        result = run(find_app, echo=self.stdout.write)
        if not result.ok:
            raise CommandError(result.failure)
