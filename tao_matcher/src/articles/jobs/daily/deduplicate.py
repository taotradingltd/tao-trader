from django_extensions.management.jobs import DailyJob
from django.db.models import Count

from articles import models

class Job(DailyJob):
    help = "Deduplicate Meltwater articles."

    def execute(self):
        duplicate_urls = [
            x["url"] for x in (models.Article.objects.values("url")
            .annotate(url_count=Count("url"))
            .filter(url_count__gt=1))
        ]

        for url in duplicate_urls:
            articles = models.Article.objects\
                .filter(url=url).order_by("date_added")

            for article in articles[1:]:
                article.delete()
