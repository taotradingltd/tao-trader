from django.contrib.auth.decorators import login_required
from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),

    # Article views
    path("create_article/", login_required(views.ArticleCreateView.as_view()), name="create_article"),
    path("delete_article/<pk>/", login_required(views.ArticleDeleteView.as_view()), name="delete_article"),
    path("edit_article/<pk>/", login_required(views.ArticleUpdateView.as_view()), name="edit_article"),

    # TODO: this should be an API endpoint, rather than a view - https://www.django-rest-framework.org/
    path("summarize_article/<pk>/", login_required(views.summarize), name="summarize_article"),

    path("upload_file/", login_required(views.MultipleFileFormView.as_view()), name="upload_file"),
]
