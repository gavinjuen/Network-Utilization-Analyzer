from django.contrib import admin
from .models import UploadRun, RingSummary, Link100GSummary


@admin.register(UploadRun)
class UploadRunAdmin(admin.ModelAdmin):
    list_display = ("id", "file_name", "uploaded_at")
    ordering = ("-uploaded_at",)


@admin.register(RingSummary)
class RingSummaryAdmin(admin.ModelAdmin):
    list_display = ("ring", "board_pair", "total_tx_gbps", "total_avg_gbps", "peak_average_ratio", "util_percent", "util_band")
    search_fields = ("ring", "board_pair", "link_instance")
    list_filter = ("util_band",)


@admin.register(Link100GSummary)
class Link100GSummaryAdmin(admin.ModelAdmin):
    list_display = ("link_name", "peak_util_gbps", "average_util_gbps", "peak_average_ratio", "util_band")
    search_fields = ("link_name", "source_site", "sink_site")
    list_filter = ("util_band",)
