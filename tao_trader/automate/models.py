from django.db import models
from django.conf import settings
from django_countries.fields import CountryField
from django.utils.translation import gettext_lazy as _
from tao_utils import tao_security
import os
from dotenv import load_dotenv
load_dotenv()

# class SiteTypes(models.TextChoices):
#     bookmaker = "bookmaker", _("bookmaker")
#     exchange = "exchange", _("exchange")

# Create your models here.
class Site(models.Model):
    SITE_TYPES = (
        ("bookmaker", "Bookmaker"),
        ("exchange", "Exchange")
    )
    name = models.CharField(max_length=200)
    url = models.CharField(max_length=200)
    country = CountryField(default='GB')
    type = models.CharField(default='bookmaker', choices=SITE_TYPES, max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "site"

    def __str__(self):
        return self.name

class Account(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    site = models.ForeignKey(Site, on_delete=models.CASCADE)
    username = models.CharField(max_length=200)
    password = models.CharField(max_length=200, db_column='password', verbose_name='Password')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "account"

    def __str__(self):
        return f"{self.user.first_name} {self.user.last_name} {self.site.name}"
    
    def save(self, *args, **kwargs):
        self.password = tao_security.encrypt(os.getenv("LADBROKES_PW_KEY"), self.password)
        super(Account, self).save(*args, **kwargs)

    def decrypted_password(self, obj):
        self.password = tao_security.decrypt(os.getenv("LADBROKES_PW_KEY"), self.password)
        return self.password
