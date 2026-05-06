from django.urls import path
from . import views

urlpatterns = [
    path("", views.upload_view, name="upload"),
    path("result/", views.result_view, name="result"),
    path("result/proof-data/", views.proof_data_view, name="proof_data"),
    path("download/excel/", views.download_excel_view, name="download_excel"),
]
