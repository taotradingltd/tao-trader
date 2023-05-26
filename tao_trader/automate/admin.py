from django.contrib import admin
from .models import Account, Site
from tao_utils import tao_security
import os

# Register your models here.

admin.site.register(Account)
admin.site.register(Site)

