from django.contrib.auth.decorators import login_required
from django.urls import path

from . import views

urlpatterns = [
    # Root view
    path("", views.index, name="index"),

    # Standard model views
    path("people/", views.people, name="people"),
    path("companies/", views.company, name="companies"),
    path("recent_updates/", views.recent_updates, name="recent_updates"),
    path("career/<pk>/", views.career, name="career"),
    path("current_employees/<pk>/", views.current_employees, name="current_employees"),

    # Model editing views
    path("create_person/", login_required(views.PersonCreateView.as_view()), name="create_person"),
    path("edit_person/<pk>/", login_required(views.PersonUpdateView.as_view()), name="edit_person"),
    path("create_company/", login_required(views.CompanyCreateView.as_view()), name="create_company"),
    path("edit_company/<pk>/", login_required(views.CompanyUpdateView.as_view()), name="edit_company"),
    path("company_merge/<pk>/", views.company_merge, name="company_merge"),

    # API style endpoints
    path("company_names/", views.get_company_names, name="company_names"),
    path("company_ids/", views.get_company_ids, name="company_ids"),

    # Utility views
    path("upload/", login_required(views.MultipleFileFormView.as_view()), name="upload"),
    path("statistics/", views.statistics, name="statistics"),
    path("validate_email/", views.validate_email, name="validate_email"),
    path("scrape/", login_required(views.ScrapingFormView.as_view()), name="scrape"),

    # Company report views
    path("report_builder/", views.report_builder, name="report_builder"),
    path("report/<pk>/", views.report, name="report"),
]
