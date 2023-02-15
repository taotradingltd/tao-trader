from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.views.generic.edit import CreateView, DeleteView, FormView, UpdateView

import django_tables2 as tables

import os
import shutil
import threading

import pandas as pd

from .forms import ArticleForm, ArticleUpdateForm, UploadForm
from .meltwater import meltwater_query
from .models import Article
from .tables import ArticleTable

class MultipleFileFormView(FormView):
    """Form view which accepts multiple files for uploading.

    TODO: use S3 as media server
    """

    form_class = UploadForm
    template_name = "articles/form.html"
    success_url = "/articles"

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form = self.get_form(form_class)
        files = request.FILES.getlist("files")
        if form.is_valid() and len(files) <= 4:
            for f in files:
                fname = f.temporary_file_path().split("/")[-1].split("\\")[-1]
                dest = settings.MEDIA_ROOT / f"{fname}"

                shutil.copyfile(f.temporary_file_path(), dest)

                args = [[], [], [], [], None, None, dest]
                t = threading.Thread(target=meltwater_query, args=args)
                t.setDaemon(True)
                t.start()
            return self.form_valid(form)
        else:
            return self.form_invalid(form)

class ArticleCreateView(CreateView):
    model = Article
    form_class = ArticleForm
    template_name = "articles/form.html"
    success_url = "/articles"

    def post(self, request, *args, **kwargs):
        if "cancel" in request.POST:
            url = self.success_url
            return HttpResponseRedirect(url)
        else:
            return super(ArticleCreateView, self).post(request, *args, **kwargs)

class ArticleDeleteView(DeleteView):
    model = Article
    template_name = "articles/form.html"
    success_url = "/articles"

    def post(self, request, *args, **kwargs):
        if "cancel" in request.POST:
            url = self.success_url
            return HttpResponseRedirect(url)
        else:
            return super(ArticleDeleteView, self).post(request, *args, **kwargs)

class ArticleUpdateView(UpdateView):
    model = Article
    form_class = ArticleUpdateForm
    template_name = "articles/form.html"
    success_url = "/articles"

    def post(self, request, *args, **kwargs):
        if "cancel" in request.POST:
            url = self.success_url
            return HttpResponseRedirect(url)
        else:
            return super(ArticleUpdateView, self).post(request, *args, **kwargs)

@login_required
def summarize(request, pk):
    # FIXME: this isn't very good
    from django.db import transaction
    import openai
    openai.api_key = os.environ["OPENAI_KEY"]
    with transaction.atomic():
        article = Article.objects.get(pk=pk)
        content = article.content

        # process with curie
        try:
            open_ai = openai.Completion.create(
                engine="curie",
                prompt=f"{content}\n\ntl;dr:",
                temperature=0.1,
                max_tokens=1000,
                top_p=1.0,
                frequency_penalty=0.37,
                presence_penalty=0.0
            )["choices"][0]["text"]
        except:
            open_ai = content

        article.edited_content = open_ai
        article.save()
    return HttpResponseRedirect(request.META.get("HTTP_REFERER", "/"))

@login_required
def index(request):
    search_terms = [
        "title",
        "source",
        "publish_date",
        "moves",
    ]

    a = Article.objects.all()
    s = False
    message = ""

    if request.POST:
        settings.LOGGER.info(request.POST)
        if "publish" in request.POST:
            from django.db import transaction
            for select in request.POST.getlist("select"):
                with transaction.atomic():
                    to_update = Article.objects.filter(pk=select)
                    u = to_update[0].publish
                    to_update.update(publish=not u)
        elif "delete" in request.POST:
            from django.db import transaction
            for select in request.POST.getlist("select"):
                with transaction.atomic():
                    Article.objects.filter(pk=select).delete()
    if request.GET:
        for term in search_terms:
            value = request.GET.get(term)
            if value:
                if term == "title":
                    a = a.filter(title__icontains=value)
                    s = True
                if term == "source":
                    a = a.filter(source__icontains=value)
                    s = True
                if term == "publish_date":
                    a = a.filter(publish_date__gte=value)
                    s = True
                if term == "moves":
                    a = a.exclude(source="Linkedin")

    if s:
        a = a.distinct()
        message = f"{len(a)} results found."

    buttons ="""<button type="submit" id="publish" name="publish" form="useful">Publish selected articles</button>
    <button type="submit" id="delete" name="delete" form="useful">Delete selected articles</button>"""
    table = ArticleTable(a, order_by=["-date_added", "-publish_date", "url"])
    tables.config.RequestConfig(request, paginate={"per_page": 25}).configure(table)
    return render(request, "articles/table.html", {"table": table, "buttons": buttons, "message": message})
