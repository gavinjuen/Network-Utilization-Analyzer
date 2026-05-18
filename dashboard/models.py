from django.db import models


class UploadRun(models.Model):
    file_name = models.CharField(max_length=500)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.file_name} - {self.uploaded_at:%Y-%m-%d %H:%M}"


class RingSummary(models.Model):
    upload_run = models.ForeignKey(UploadRun, on_delete=models.CASCADE, related_name="ring_summaries")
    ring = models.TextField()
    board_pair = models.TextField()
    link_instance = models.TextField(blank=True)
    peak_time = models.TextField(blank=True)
    endpoint_1 = models.TextField(blank=True)
    tx_1_gbps = models.FloatField(default=0)
    endpoint_2 = models.TextField(blank=True)
    tx_2_gbps = models.FloatField(default=0)
    total_tx_gbps = models.FloatField(default=0)
    avg_endpoint_1_gbps = models.FloatField(default=0)
    avg_endpoint_2_gbps = models.FloatField(default=0)
    total_avg_gbps = models.FloatField(default=0)
    peak_average_ratio = models.FloatField(default=0)
    max_capacity_gbps = models.FloatField(default=0)
    util_percent = models.FloatField(default=0)
    util_band = models.CharField(max_length=50, blank=True)

    def __str__(self):
        return f"{self.ring} | {self.board_pair}"


class Link100GSummary(models.Model):
    upload_run = models.ForeignKey(UploadRun, on_delete=models.CASCADE, related_name="link100g_summaries")
    link_name = models.TextField()
    source_site = models.TextField(blank=True)
    sink_site = models.TextField(blank=True)
    peak_time = models.TextField(blank=True)
    peak_util_gbps = models.FloatField(default=0)
    average_util_gbps = models.FloatField(default=0)
    peak_average_ratio = models.FloatField(default=0)
    util_band = models.CharField(max_length=50, blank=True)
    
    def __str__(self):
        return f"{self.link_name} | {self.peak_util_gbps}Gbps"
