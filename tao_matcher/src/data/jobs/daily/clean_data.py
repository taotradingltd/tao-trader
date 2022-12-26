import datetime

from django.conf import settings
from django_extensions.management.jobs import DailyJob

from data.models import Career, PersonEmail

class Job(DailyJob):
    help = 'Clean out duplicate data.'

    def execute(self):
        dedupe_career()
        remove_faulty_emails()

# =============================
# UTILITY FUNCTIONS
# =============================

def remove_faulty_emails():
    PersonEmail.objects.filter(value__startswith="@").delete()

def dedupe_career():
    # Get all duplicates in career on person, company, role
    items = Career.objects.raw("SELECT id FROM career GROUP BY person_id, company_id, role HAVING COUNT(*)>1;")

    for item in items:
        dels = Career.objects.filter(person=item.person, company=item.company_name, role=item.role, date_started=datetime.date(1970, 1, 1), date_ended=datetime.date(1970, 1, 1))
        dels.delete()

        dupes = Career.objects.filter(person=item.person, company=item.company_name, role=item.role)

        if len(dupes) != 1:
            entries = {}

            for dupe in dupes:
                try:
                    entries[(dupe.date_started.year, dupe.date_ended.year)].append(dupe)
                except KeyError:
                    entries[(dupe.date_started.year, dupe.date_ended.year)] = []
                    entries[(dupe.date_started.year, dupe.date_ended.year)].append(dupe)

            for pair in entries:
                avail = entries[pair]

                if len(avail) > 1:
                    dels = avail[1:]
                    for d in dels:
                        d.delete()
                    try:
                        avail[0].date_started = avail[0].date_started.replace(day=1, month=1)
                        avail[0].date_ended = avail[0].date_ended.replace(day=1, month=1)
                        avail[0].save()
                    except:
                        settings.LOGGER.info(f'Failed to save {avail[0]}')
                        continue
