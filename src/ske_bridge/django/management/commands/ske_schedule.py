"""``python manage.py ske_schedule``: the platform's scheduler command. It sends
the Celery beat entries due this minute; see ske_bridge.celery.scheduler."""

from django.core.management.base import BaseCommand, CommandError

from ske_bridge.celery import find_app
from ske_bridge.celery.scheduler import tick


class Command(BaseCommand):
    help = "Send the Celery beat schedule's entries that are due this minute."

    def handle(self, *args, **options):
        result = tick(find_app(), echo=self.stdout.write)
        if not result.ok:
            raise CommandError(f"{len(result.failed)} scheduled task(s) could not be sent")
