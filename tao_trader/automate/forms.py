from django import forms
from models import *

class AccountForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput)
    class Meta:
        model = Account
        widgets = {
        'password': forms.PasswordInput(),
    }
    # class Meta:
    #     model = Account
    #     fields = ('user', 'site', 'username', 'password')
