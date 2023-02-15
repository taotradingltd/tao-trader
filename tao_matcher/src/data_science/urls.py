from django.contrib.auth.decorators import login_required
from django.urls import path

from . import views

urlpatterns = [
    # Root view
    path('', views.index, name='index'),

    path('newsletter/', views.newsletter_data, name='newsletter'),
    path('newsletter_data/<pub>/', views.get_newsletter_data, name='newsletter_data'),

    path('moves/', login_required(views.MoveReportRequest.as_view()), name='report_request'),
    path('moves/<year>/<month>/', login_required(views.people_moves), name='moves_for_month'),

    path('badges/', login_required(views.Badges.as_view()), name='badges'),
]
