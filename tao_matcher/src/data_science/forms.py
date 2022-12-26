import datetime
from django import forms
from django.core.validators import FileExtensionValidator

class PeopleMoveReportGenerator(forms.Form):
    month = forms.IntegerField(widget=forms.Select(choices=[(i, i) for i in range(1, 13)]), initial=1)
    year = forms.IntegerField(widget=forms.Select(
        choices=[(i, i) for i in range(datetime.datetime.now().date().year-5, datetime.datetime.now().date().year+1)]),
        initial=datetime.datetime.now().date().year,
    )

class BadgesForm(forms.Form):
    file = forms.FileField(
        label="",
        allow_empty_file=False,
        validators=[FileExtensionValidator(allowed_extensions=["xlsx"])],
    )
    roundtables = forms.IntegerField(widget=forms.Select(choices=[(i, i) for i in range(1, 11)]), initial=1)
    sessions = forms.IntegerField(widget=forms.Select(choices=[(i, i) for i in range(1, 11)]), initial=1)
    blanks = forms.IntegerField(widget=forms.Select(choices=[(i, i) for i in range(0, 401, 50)]), initial=250)

    # Advanced settings
    seed = forms.IntegerField(widget=forms.Select(choices=[(i, i) for i in range(1, 11)]), initial=3,
        label="Seed (DON'T CHANGE WITHOUT GUIDANCE)")
    deviation = forms.IntegerField(widget=forms.Select(choices=[(i, i) for i in range(1, 101)]), initial=1,
        label="Deviation (DON'T CHANGE WITHOUT GUIDANCE)")
    iterations = forms.IntegerField(widget=forms.Select(choices=[(i, i) for i in range(1, 10)]), initial=5,
        label="Iterations (DON'T CHANGE WITHOUT GUIDANCE)")

    def __init__(self, *args, **kwargs):
        super(BadgesForm, self).__init__(*args, **kwargs)
        self.label_suffix = ""
