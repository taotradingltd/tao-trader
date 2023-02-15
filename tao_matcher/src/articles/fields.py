from django.db import models

TITLE_CHOICES = [
    (0, "—"),
    (1, "HW"),
    (2, "HW (?)"),
    (3, "PEW"),
    (4, "PEW (?)")
]

class MediumTextField(models.Field):
    def db_type(self, connection):
        return "MEDIUMTEXT"
