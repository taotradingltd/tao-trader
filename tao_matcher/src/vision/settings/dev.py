from .common import *

import os
if os.getenv("LOCAL"):
    del DATABASES["default"]["OPTIONS"]
    DATABASES["default"]["ENGINE"] = "django.db.backends.sqlite3"
    DATABASES["default"]["NAME"] = PROJECT_ROOT / (DATABASES["default"]["NAME"] + ".db")
else:
    DATABASES["default"]["ENGINE"] = "django.db.backends.mysql"


# Django Debug Toolbar

INTERNAL_IPS = ["127.0.0.1"]
INSTALLED_APPS += ["debug_toolbar"]
MIDDLEWARE += ["debug_toolbar.middleware.DebugToolbarMiddleware"]

import debug_toolbar
def show_toolbar_to_staff(request):
    return request.user.is_staff and debug_toolbar.middleware.show_toolbar(request)

DEBUG_TOOLBAR_CONFIG = {
    "SHOW_TOOLBAR_CONFIG": show_toolbar_to_staff
}
