from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User

class RegistrationForm(UserCreationForm):
    """Registration form to allow GFM staff to register for access to Vision.

    TODO: email confirmation before creating the user
    """
    email = forms.EmailField(required=True)

    def clean_email(self):
        """Cleans the email address according to the following criteria:
            - Emails should be for GFM domains
            - Emails should not contain comma's, or spaces
            - Emails should not already exist i.e. one account per person

        TODO: try to authenticate users are real GFM emails
        """
        data = self.cleaned_data["email"].strip()
        if User.objects.filter(email=data).exists():
            raise forms.ValidationError("This email address is already in use.")
        if "," in data or " " in data:
            raise forms.ValidationError("This is not a valid email address.")
        if not data.split("@")[1] == "globalfundmedia.com":
            raise forms.ValidationError("Sorry, GFM only.")
        return data

    def save(self, commit=True):
        user = super(RegistrationForm, self).save(commit=False)
        user.email = self.cleaned_data["email"]
        if commit:
            user.save()
        return user

    class Meta:
        model = User
        fields = ("username", "email", "password1", "password2")
