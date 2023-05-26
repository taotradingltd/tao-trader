from django.shortcuts import render

from django.http import HttpResponse

from .models import Site

def index(request):
    return HttpResponse("Hello, world. You're at the Tao Automate index.")
