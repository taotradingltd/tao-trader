from django.shortcuts import render
from django.http import HttpResponseRedirect
from django.contrib.auth.decorators import login_required

from .forms import RegistrationForm

def register(request):
    """RegistrationForm view.

    TODO: make class-based view i.e.
https://docs.djangoproject.com/en/4.0/topics/class-based-views/generic-editing
    """
    message = ""
    form = RegistrationForm()
    if request.POST:
        form = RegistrationForm(request.POST)
        if form.is_valid():
            form.save()
            return HttpResponseRedirect("/articles")
        message = "Unsuccessful registration. Invalid information."

    return render(
        request,
        template_name="registration/registration.html",
        context={"form": form, "message": message}
    )

@login_required
def index(request):
    return render(request, template_name="index.html")
