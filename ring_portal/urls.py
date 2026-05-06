from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from . import views
urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("dashboard.urls")),
    path("", views.upload_view, name="upload"),
    path("result/", views.result_view, name="result"),
    path("proof-data/", views.proof_data_view, name="proof_data"),
    path("download-excel/", views.download_excel_view, name="download_excel"),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
