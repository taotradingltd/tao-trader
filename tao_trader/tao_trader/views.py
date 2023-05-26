from django.shortcuts import render

from django.http import HttpResponse

def index(request):
    return HttpResponse('Hello, welcome to the Tao Trader home page.')

