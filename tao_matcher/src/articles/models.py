from django.db import models
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from . import fields

class Kind(models.TextChoices):
    """Where the article came from."""
    NONE = "—", _("—")

    MW = "Meltwater", _("Meltwater")
    GA = "Google Alerts", _("Google Alerts")

class Title(models.TextChoices):
    """GFM editorial titles."""
    NONE = "—", _("—")

    HW = "HW", _("HW")
    PEW = "PEW", _("PEW")
    HW_Q = "HW (?)", _("HW (?)")
    PEW_Q = "PEW (?)", _("PEW (?)")

class Article(models.Model):
    title = models.TextField(null=True, blank=True, db_column="title")
    content = fields.MediumTextField(null=True, blank=True, db_column="content")
    author = models.CharField(max_length=100, null=True, blank=True, db_column="author")
    url = models.CharField(max_length=255, null=True, blank=True, db_column="url")
    source = models.CharField(max_length=255, null=True, blank=True, db_column="source")

    # TODO: https://django-taggit.readthedocs.io/en/latest/
    tags = models.CharField(max_length=255, null=True, blank=True, db_column="tags")

    publish_date = models.DateField(blank=False, db_column="publish_date")
    publish = models.BooleanField(default=0, db_column="publish")
    editorial_title = models.CharField(max_length=50, choices=Title.choices, default=None, db_column="editorial_title")
    date_modified = models.DateTimeField(auto_now=True)
    date_added = models.DateTimeField(auto_now_add=True)
    kind = models.CharField(max_length=50, choices=Kind.choices, default=None, db_column="kind")

    def __str__(self):
        return f"{self.title}"

    @cached_property
    def has_url(self):
        # Property used for rendering urls as 'clickable'
        if "djp:" not in self.url[:5]:
            return True
        return False

    @cached_property
    def has_source_name(self):
        if self.source:
            return True
        return False

    @cached_property
    def get_source_name(self):
        if self.has_source_name:
            return self.source.replace("- Powered by Dow Jones", "").strip()
        return "-"

    class Meta:
        db_table = "article"
