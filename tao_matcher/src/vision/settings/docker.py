from .common import *

import socket

hostname, _, ips = socket.gethostbyname_ex(socket.gethostname())
INTERNAL_IPS = [ip[: ip.rfind(".")] + ".1" for ip in ips] \
    + ["127.0.0.1", "10.0.2.2"]

ARTIFACTS_DIR = BASE_DIR / "artifacts"
MODELS_DIR = ARTIFACTS_DIR / "models"

DATABASES["default"]["ENGINE"] = "django.db.backends.mysql"
