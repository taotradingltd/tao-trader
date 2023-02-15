import datetime

from django import forms
from django.core.validators import FileExtensionValidator
from djrichtextfield.widgets import RichTextWidget

from .models import Article, Title

class ArticleForm(forms.ModelForm):
    title = forms.CharField()
    content = forms.CharField(widget=RichTextWidget)
    url = forms.CharField(widget=forms.TextInput)
    source = forms.CharField(widget=forms.TextInput)
    editorial_title = forms.ChoiceField(choices=[x for x in Title.choices], widget=forms.RadioSelect)
    publish_date = forms.DateField(widget=forms.SelectDateWidget, initial=datetime.date.today)
    tags = forms.CharField()

    def __init__(self, *args, **kwargs):
        super(ArticleForm, self).__init__(*args, **kwargs)
        self.fields["title"].required = False
        self.fields["content"].required = False
        self.fields["author"].required = False
        self.fields["source"].required = False
        self.fields["tags"].required = False
        self.fields["editorial_title"].required = False

        self.label_suffix = ""

    class Meta:
        model = Article
        exclude = ("publish", "kind", "date_modified", "date_added")

class ArticleUpdateForm(ArticleForm):
    url = forms.CharField(widget=forms.TextInput(attrs={"readonly":"readonly"}))
    source = forms.CharField(widget=forms.TextInput(attrs={"readonly":"readonly"}))
    publish_date = forms.DateField(widget=forms.SelectDateWidget(attrs={"readonly":"readonly"}))

    def __init__(self, *args, **kwargs):
        super(ArticleUpdateForm, self).__init__(*args, **kwargs)

class UploadForm(forms.Form):
    files = forms.FileField(
        label="",
        allow_empty_file=False,
        validators=[FileExtensionValidator(allowed_extensions=["csv"])],
        widget=forms.FileInput(attrs={"multiple": True})
    )

    def clean_files(self):
        limit = 3
        files = self.files.getlist("files")
        if len(files) > limit:
            raise forms.ValidationError(f"Please upload a maximum of {limit} files.")
        return files
