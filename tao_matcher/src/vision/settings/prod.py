from .common import *

DEBUG = False

CSRF_COOKIE_SECURE = True
SESSION_COOKIE_SECURE = True

STATIC_ROOT = "/var/www/gfm-vision/static"
MEDIA_ROOT = "/var/www/gfm-vision/media"

ALLOWED_HOSTS = ["vision.globalfundmedia.com"]

DATABASES["default"]["ENGINE"] = "django.db.backends.mysql"
