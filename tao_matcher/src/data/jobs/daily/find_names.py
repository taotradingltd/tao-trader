from django_extensions.management.jobs import DailyJob

from django.conf import settings

from gfm_utils.text.preprocessing import clean_name

from data.models import  Company

class Job(DailyJob):
    help = 'Fill in blank names in the company table.'

    def execute(self):
        objs_to_check = Company.objects.filter(name__isnull=True) | Company.objects.filter(name__exact="")
        objs_to_check = objs_to_check.exclude(pk=-1)

        total = len(objs_to_check)
        settings.LOGGER.info(f"Checking for name fixes on {total} companies")
        count = 0

        # Find the most repeated name for a given company in the career table
        for obj in objs_to_check:
            count += 1

            if count % (total / 10) == 0:
                settings.LOGGER.info(f"{(count/total) * 100}% complete")
            counts = obj.name_appearances

            if len(counts) < 1:
                continue

            _highest = counts[0]
            _second = counts[1] if len(counts) > 1 else ("", 0)

            if _highest:
                if _highest[1] > _second[1] and _highest[1] > 1:
                    obj.name = _highest[0]
                    obj.cleaned_name = clean_name(_highest[0], "company", True, False)
                    obj.save()

        # Final clean up of cleaned_name
        objs_to_check = Company.objects.all().exclude(name__isnull=True).exclude(name__exact="")
        objs_to_check = objs_to_check.filter(cleaned_name__isnull=True) | objs_to_check.filter(cleaned_name__exact="")

        total = len(objs_to_check)
        settings.LOGGER.info(f"Cleaning names of {total} companies")
        count = 0

        for obj in objs_to_check:
            count += 1

            if count % (total / 10) == 0:
                settings.LOGGER.info(f"{(count/total) * 100}% complete")

            obj.cleaned_name = clean_name(obj.name, "company", True, False)
            obj.save()
