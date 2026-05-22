from django.urls import path
from . import views

urlpatterns = [
    path("", views.upload_view, name="upload"),
    path("result/", views.result_view, name="result"),
    path("weekly-trend/", views.weekly_trend_view, name="weekly_trend"),
    path("proof-data/", views.proof_data_view, name="proof_data"),
    path("download-excel/", views.download_excel_view, name="download_excel"),
    path(
    "api/ring-trend/",
    views.ring_trend_api,
    name="ring_trend_api"
    ),
    path("ring-nodes/", views.ring_nodes_view, name="ring_nodes")
]