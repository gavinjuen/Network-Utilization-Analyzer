from __future__ import annotations

from pathlib import Path
from uuid import uuid4
import time
import json
from collections import defaultdict
import time
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.db.models import Avg, Max
from django.db.models.functions import TruncWeek
from django.template.loader import render_to_string
import pandas as pd
from collections import defaultdict
import json
import pandas as pd
from .models import RingSummary, Link100GSummary,AccessBandwidth
from .forms import UploadFilesForm, RingMappingUploadForm,BandwidthMasterUploadForm
from .models import UploadRun, RingSummary, Link100GSummary, RingNodeMapping,FTTHSummary
from .calculations import (
    read_uploaded_files, prepare_dataframe, build_ring_peak_summary,
    build_100g_peak_summary, build_ring_proof, build_100g_proof, to_excel_bytes,
    build_service_peak_summary,build_access_service_summary,build_ftth_peak_summary, normalize_ring_name,
    normalize_board_pair,parse_resource_once
)
from django.contrib import messages
import math

CACHE_MAX_AGE_SECONDS = 60 * 60 * 12
def parse_huawei_datetime(value):

    if pd.isna(value):
        return pd.NaT

    value = str(value).strip().strip("'").strip('"')

    return pd.to_datetime(
        value,
        dayfirst=True,
        errors="coerce"
    )

def _safe_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default

def upload_ring_mapping_view(request):
    form = RingMappingUploadForm()

    if request.method == "POST":
        form = RingMappingUploadForm(request.POST, request.FILES)

        if form.is_valid():
            mapping_file = request.FILES["mapping_file"]

            df = pd.read_excel(mapping_file)
            df.columns = df.columns.str.strip()

            required = ["Link Group Name", "Link Group", "Source NE", "Sink NE"]
            missing = [c for c in required if c not in df.columns]

            if missing:
                messages.error(request, f"Missing columns: {missing}")
            else:
                RingNodeMapping.objects.all().delete()

                objects = []

                for _, row in df.iterrows():
                    link_group_name = str(row["Link Group Name"]).strip()
                    board_pair = str(row["Link Group"]).strip()
                    source_ne = str(row["Source NE"]).strip()
                    sink_ne = str(row["Sink NE"]).strip()

                    if not link_group_name or link_group_name.lower() == "nan":
                        continue

                    objects.append(RingNodeMapping(
                        link_group_name=link_group_name,
                        normalized_ring=normalize_ring_name(link_group_name),
                        board_pair=board_pair,
                        normalized_board_pair=normalize_board_pair(board_pair),
                        source_ne=source_ne,
                        sink_ne=sink_ne,
                    ))

                RingNodeMapping.objects.bulk_create(objects, batch_size=1000)

                messages.success(
                    request,
                    f"Imported {len(objects)} ring node mappings successfully."
                )

    return redirect("upload")


def upload_bandwidth_master_view(request):
    if request.method != "POST":
        return redirect("upload")

    file = request.FILES.get("bandwidth_file")

    if not file:
        messages.error(request, "No bandwidth masterlist file uploaded.")
        return redirect("upload")

    try:
        df = pd.read_excel(file)
        df.columns = df.columns.astype(str).str.strip().str.upper()

        required_cols = ["NEID", "SITE ID", "SITE NAME", "CURRENT CAPACITY"]
        missing = [c for c in required_cols if c not in df.columns]

        if missing:
            messages.error(request, f"Missing required columns: {', '.join(missing)}")
            return redirect("upload")

        objects = []

        for _, row in df.iterrows():
            site_name = str(row.get("SITE NAME", "")).upper().strip()

            if not site_name or site_name == "NAN":
                continue

            capacity_raw = str(row.get("CURRENT CAPACITY", "0")).replace(",", "").strip()

            try:
                capacity = float(capacity_raw)
                if math.isnan(capacity):
                    capacity = 0
            except (ValueError, TypeError):
                capacity = 0

            objects.append(AccessBandwidth(
                neid=str(row.get("NEID", "")).strip(),
                site_id=str(row.get("SITE ID", "")).strip(),
                site_name=site_name,
                current_capacity=capacity,  # Mbps
            ))

        AccessBandwidth.objects.all().delete()
        AccessBandwidth.objects.bulk_create(objects, batch_size=1000)

        messages.success(request, f"Imported {len(objects)} bandwidth records successfully.")

    except Exception as e:
        messages.error(request, f"Failed to import bandwidth masterlist: {e}")

    return redirect("upload")

def _save_analysis_to_db(files, ring_peaks: pd.DataFrame, g100_peaks: pd.DataFrame,ftth_peaks: pd.DataFrame = None) -> UploadRun:
    file_name = ", ".join([getattr(f, "name", str(f)) for f in files]) or "Unknown upload"
    upload_run = UploadRun.objects.create(file_name=file_name)

    ring_objects = []
    for _, row in ring_peaks.fillna("").iterrows():
        ring_objects.append(RingSummary(
            upload_run=upload_run,
            ring=str(row.get("Ring", "")),
            board_pair=str(row.get("Board Pair", "")),
            link_instance=str(row.get("Link Instance", "")),
            peak_time=row.get("Peak Time"),
            endpoint_1=str(row.get("Endpoint 1", "")),
            tx_1_gbps=_safe_float(row.get("TX 1 (Gbps)", 0)),
            endpoint_2=str(row.get("Endpoint 2", "")),
            tx_2_gbps=_safe_float(row.get("TX 2 (Gbps)", 0)),
            total_tx_gbps=_safe_float(row.get("Total TX (Gbps)", 0)),
            avg_endpoint_1_gbps=_safe_float(row.get("Avg Endpoint 1 (Gbps)", 0)),
            avg_endpoint_2_gbps=_safe_float(row.get("Avg Endpoint 2 (Gbps)", 0)),
            total_avg_gbps=_safe_float(row.get("Total Avg (Gbps)", row.get("Average (Gbps)", 0))),
            peak_average_ratio=_safe_float(row.get("Peak Average Ratio", 0)),
            max_capacity_gbps=_safe_float(row.get("Max Capacity (Gbps)", 0)),
            util_percent=_safe_float(row.get("Util %", 0)),
            util_band=str(row.get("Util Band", "")),
        ))
    if ring_objects:
        RingSummary.objects.bulk_create(ring_objects, batch_size=500)

    g100_objects = []
    for _, row in g100_peaks.fillna("").iterrows():
        g100_objects.append(Link100GSummary(
            upload_run=upload_run,
            link_name=str(row.get("100G Link", "")),
            source_site=str(row.get("Source Site", "")),
            sink_site=str(row.get("Sink Site", "")),
            peak_time=row.get("Peak Time"),
            peak_util_gbps=_safe_float(row.get("Peak Util (Gbps)", 0)),
            average_util_gbps=_safe_float(row.get("Average Util (Gbps)", 0)),
            peak_average_ratio=_safe_float(row.get("Peak Average Ratio", 0)),
            util_band=str(row.get("Util Band", "")),
        ))
    if g100_objects:
        Link100GSummary.objects.bulk_create(g100_objects, batch_size=500)

    if ftth_peaks is not None and not ftth_peaks.empty:
        ftth_objects = []

    for _, row in ftth_peaks.fillna("").iterrows():
        ftth_objects.append(FTTHSummary(
            upload_run=upload_run,
            olt_name=str(row.get("OLT Name", "")),
            peak_time=row.get("Peak Time"),
            peak_util_gbps=_safe_float(row.get("Peak Util (Gbps)", 0)),
            average_util_gbps=_safe_float(row.get("Average Util (Gbps)", 0)),
            peak_direction=str(row.get("Peak Direction", "")),
        ))

    if ftth_objects:
        FTTHSummary.objects.bulk_create(ftth_objects, batch_size=500)

    return upload_run


def _latest_db_results():
    latest_run = UploadRun.objects.order_by("-uploaded_at").first()

    if not latest_run:
        return None, None, None, None

    ring_rows = []

    for row in RingSummary.objects.filter(upload_run=latest_run):
        ring_rows.append({
            "Ring": row.ring,
            "Board Pair": row.board_pair,
            "Link Instance": row.link_instance,
            "Peak Time": row.peak_time,
            "Endpoint 1": row.endpoint_1,
            "TX 1 (Gbps)": row.tx_1_gbps,
            "Endpoint 2": row.endpoint_2,
            "TX 2 (Gbps)": row.tx_2_gbps,
            "Total TX (Gbps)": row.total_tx_gbps,
            "Avg Endpoint 1 (Gbps)": row.avg_endpoint_1_gbps,
            "Avg Endpoint 2 (Gbps)": row.avg_endpoint_2_gbps,
            "Total Avg (Gbps)": row.total_avg_gbps,
            "Peak Average Ratio": row.peak_average_ratio,
            "Max Capacity (Gbps)": row.max_capacity_gbps,
            "Util %": row.util_percent,
            "Util Band": row.util_band,
        })

    g100_rows = []

    for row in Link100GSummary.objects.filter(upload_run=latest_run):
            g100_rows.append({
        "100G Link": row.link_name,
        "Source Site": row.source_site,
        "Sink Site": row.sink_site,
        "Peak Time": row.peak_time,
        "Peak Util (Gbps)": row.peak_util_gbps,
        "Average Util (Gbps)": row.average_util_gbps,
        "BH Avg 20-22 (Gbps)": 0,
        "Peak Average Ratio": row.peak_average_ratio,
        "Util Band": row.util_band,
    })
            
    ftth_rows = []

    for row in FTTHSummary.objects.filter(upload_run=latest_run):
        ftth_rows.append({
            "OLT Name": row.olt_name,
            "Peak Time": row.peak_time,
            "Peak Util (Gbps)": row.peak_util_gbps,
            "Average Util (Gbps)": row.average_util_gbps,
            "Peak Direction": row.peak_direction,
        })

    ftth_peaks = pd.DataFrame(ftth_rows)
    ring_peaks = pd.DataFrame(ring_rows)
    g100_peaks = pd.DataFrame(g100_rows)
    df = pd.DataFrame()

    return df, ring_peaks, g100_peaks,ftth_peaks


def _json_default(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _results_dir() -> Path:
    results_dir = Path(settings.MEDIA_ROOT) / "session_cache"
    results_dir.mkdir(parents=True, exist_ok=True)
    return results_dir


def _purge_old_cache_files() -> None:
    now = time.time()
    for path in _results_dir().glob("*.pkl.gz"):
        try:
            if now - path.stat().st_mtime > CACHE_MAX_AGE_SECONDS:
                path.unlink(missing_ok=True)
        except OSError:
            pass


def _cache_file(cache_id: str, suffix: str) -> Path:
    return _results_dir() / f"{cache_id}_{suffix}.pkl.gz"


def _store_results(request, df, ring_peaks, g100_peaks, ftth_peaks) -> None:
    _purge_old_cache_files()

    old_cache_id = request.session.get("cache_id")
    if old_cache_id:
        for suffix in ("df", "ring", "g100", "ftth"):
            try:
                _cache_file(old_cache_id, suffix).unlink(missing_ok=True)
            except OSError:
                pass

    cache_id = uuid4().hex

    proof_columns = [
        "Collection Time",
        "Resource Name",
        "Source File",
        "TX_bps",
        "RX_bps",
        "MAX_bps",
        "Ring",
        "Endpoint",
        "Board Type",
        "Link Instance",
        "Service Group",
        "Service Name",   # add this
        "100G Link",
    ]
    available_cols = [c for c in proof_columns if c in df.columns]
    proof_df = df[available_cols].copy()

    proof_df.to_pickle(_cache_file(cache_id, "df"), compression="gzip")
    ring_peaks.to_pickle(_cache_file(cache_id, "ring"), compression="gzip")
    g100_peaks.to_pickle(_cache_file(cache_id, "g100"), compression="gzip")
    ftth_peaks.to_pickle(_cache_file(cache_id, "ftth"), compression="gzip")

    request.session["cache_id"] = cache_id
    request.session.modified = True

def _load_results(request):
    cache_id = request.session.get("cache_id")
    if not cache_id:
        return None, None, None, None

    df_path = _cache_file(cache_id, "df")
    ring_path = _cache_file(cache_id, "ring")
    g100_path = _cache_file(cache_id, "g100")
    ftth_path = _cache_file(cache_id, "ftth")

    if (
        not df_path.exists()
        or not ring_path.exists()
        or not g100_path.exists()
        or not ftth_path.exists()
    ):
        return None, None, None, None

    try:
        df = pd.read_pickle(df_path, compression="gzip")
        ring_peaks = pd.read_pickle(ring_path, compression="gzip")
        g100_peaks = pd.read_pickle(g100_path, compression="gzip")
        ftth_peaks = pd.read_pickle(ftth_path, compression="gzip")
    except Exception:
        return None, None, None, None

    return df, ring_peaks, g100_peaks, ftth_peaks


def _proof_context(df, ring_peaks, g100_peaks, request=None):
    context = {
        "ring_debug_options": [],
        "g100_debug_options": [],
        "selected_debug_type": "ring",
        "selected_ring_label": "",
        "selected_g100_link": "",
        "proof_ring_endpoint_columns": [],
        "proof_ring_endpoint_rows": [],
        "proof_ring_timestamp_columns": [],
        "proof_ring_timestamp_rows": [],
        "proof_ring_same_time_columns": [],
        "proof_ring_same_time_rows": [],
        "proof_ring_step3_total": None,
        "proof_g100_columns": [],
        "proof_g100_rows": [],
    }

    if not ring_peaks.empty:
        options = []
        for _, row in ring_peaks.iterrows():
            instance = "" if pd.isna(row.get("Link Instance", "")) else str(row.get("Link Instance", ""))
            board_pair = "" if pd.isna(row.get("Board Pair", "")) else str(row.get("Board Pair", ""))

            label = f"{row['Ring']} | {board_pair} | {instance if instance else 'Non-UNQ2/U220'}"

            options.append({
                "label": label,
                "ring": str(row["Ring"]),
                "board_pair": board_pair,
                "instance": instance,
            })

        context["ring_debug_options"] = options

    if not g100_peaks.empty:
        context["g100_debug_options"] = [
            str(v) for v in g100_peaks["100G Link"].dropna().tolist()
        ]

    if request is not None:
        debug_type = request.GET.get("debug", "ring")
        context["selected_debug_type"] = debug_type

        if debug_type == "g100" and context["g100_debug_options"]:
            selected_link = request.GET.get("g100_link") or context["g100_debug_options"][0]
            context["selected_g100_link"] = selected_link

            proof_100g = build_100g_proof(df, selected_link)

            if not proof_100g.empty:
                proof_100g = proof_100g.fillna("")
                cols = [
                    "Collection Time",
                    "100G Link",
                    "TX (Gbps)",
                    "RX (Gbps)",
                    "Selected Max TX/RX (Gbps)",
                    "Resource Name",
                    "Source File",
                ]
                context["proof_g100_columns"] = cols
                context["proof_g100_rows"] = proof_100g[cols].to_dict(orient="records")

        else:
            context["selected_debug_type"] = "ring"

            if context["ring_debug_options"]:
                selected_label = request.GET.get("ring_label") or context["ring_debug_options"][0]["label"]
                context["selected_ring_label"] = selected_label

                match = next(
                    (o for o in context["ring_debug_options"] if o["label"] == selected_label),
                    context["ring_debug_options"][0]
                )

                endpoint_totals, same_time, timestamp_totals = build_ring_proof(
                    df,
                    match["ring"],
                    match["board_pair"],
                    match["instance"],
                )

                if not endpoint_totals.empty:
                    endpoint_totals = endpoint_totals.fillna("")
                    same_time = same_time.fillna("")
                    timestamp_totals = timestamp_totals.fillna("")

                    context["proof_ring_endpoint_columns"] = [
                        "Collection Time",
                        "Endpoint",
                        "TX (Gbps)",
                    ]
                    context["proof_ring_endpoint_rows"] = endpoint_totals[
                        context["proof_ring_endpoint_columns"]
                    ].to_dict(orient="records")

                    context["proof_ring_timestamp_columns"] = [
                        "Collection Time",
                        "Total TX (Gbps)",
                    ]
                    context["proof_ring_timestamp_rows"] = timestamp_totals[
                        context["proof_ring_timestamp_columns"]
                    ].to_dict(orient="records")

                    context["proof_ring_same_time_columns"] = [
                        "Collection Time",
                        "Endpoint",
                        "TX (Gbps)",
                    ]
                    context["proof_ring_same_time_rows"] = same_time[
                        context["proof_ring_same_time_columns"]
                    ].to_dict(orient="records")

                    try:
                        context["proof_ring_step3_total"] = round(
                            float(same_time["TX (Gbps)"].sum()),
                            3
                        )
                    except Exception:
                        context["proof_ring_step3_total"] = None

    return context

def _build_context(df, ring_peaks, g100_peaks, service_peaks=None, errors=None, request=None):
    context = {
        "errors": errors or [],
        "ring_columns": list(ring_peaks.columns),
        "ring_rows": ring_peaks.fillna("").to_dict(orient="records"),
        "g100_columns": list(g100_peaks.columns),
        "g100_rows": g100_peaks.fillna("").to_dict(orient="records"),
        "ring_count": ring_peaks["Ring"].nunique() if not ring_peaks.empty else 0,
        "ring_row_count": len(ring_peaks),
        "g100_count": len(g100_peaks),
        "busiest_ring": float(ring_peaks["Total TX (Gbps)"].max()) if not ring_peaks.empty else 0.0,
        "busiest_100g": float(g100_peaks["Peak Util (Gbps)"].max()) if not g100_peaks.empty else 0.0,
        "top10_ring_columns": ["Ring", "Board Pair", "Total TX (Gbps)", "Util %", "Util Band"],
        "top10_ring_rows": [],
        "top10_100g_columns": ["100G Link", "Peak Util (Gbps)", "Average Util (Gbps)", "BH Avg 20-22 (Gbps)", "Peak Average Ratio", "Util Band", "Peak Time"],
        "top10_100g_rows": [],
        "top10_ring_chart_labels": [],
        "top10_ring_chart_values": [],
        "top10_100g_chart_labels": [],
        "top10_100g_chart_values": [],
        "service_columns": list(service_peaks.columns) if service_peaks is not None else [],
        "service_rows": service_peaks.fillna("").to_dict(orient="records") if service_peaks is not None else [],
        "service_count": len(service_peaks) if service_peaks is not None else 0,
    }

    if not ring_peaks.empty:
        top10_ring = ring_peaks.sort_values(["Total TX (Gbps)", "Util %"], ascending=[False, False]).head(10).fillna("")
        context["top10_ring_rows"] = top10_ring[context["top10_ring_columns"]].to_dict(orient="records")
        
        context["top10_ring_chart_labels"] = [f"{r['Ring']} {r['Link Instance']}".strip() for _, r in top10_ring.iterrows()]
        context["top10_ring_chart_values"] = [float(v) for v in top10_ring["Total TX (Gbps)"].tolist()]

    if not g100_peaks.empty:
        top10_100g = g100_peaks.sort_values(["Peak Util (Gbps)"], ascending=[False]).head(10).fillna("")
        context["top10_100g_rows"] = top10_100g[context["top10_100g_columns"]].to_dict(orient="records")
        context["top10_100g_chart_labels"] = [str(v) for v in top10_100g["100G Link"].tolist()]
        context["top10_100g_chart_values"] = [float(v) for v in top10_100g["Peak Util (Gbps)"].tolist()]
    
    if request and request.GET.get("debug"):
        context.update(_proof_context(df,ring_peaks,g100_peaks,request=request))
    else :
        context.update(_proof_context(df, ring_peaks, g100_peaks, request=None))
    return context


def upload_view(request):
    form = UploadFilesForm()
    return render(request, "dashboard/upload.html", {"form": form})


def result_view(request):
    if request.method == "POST":
        form = UploadFilesForm(request.POST, request.FILES)

        if not form.is_valid():
            errors = []
            for field, msgs in form.errors.items():
                for msg in msgs:
                    errors.append(f"{field}: {msg}")
            return render(request, "dashboard/upload.html", {"form": form, "errors": errors})

        files = form.cleaned_data["files"]
        skiprows = form.cleaned_data["skiprows"]
        raw_df, errors = read_uploaded_files(files, skiprows)

        if raw_df.empty:
            return render(request, "dashboard/upload.html", {
                "form": form,
                "errors": errors or ["No valid data loaded. Try different skiprows."],
            })
        start = time.perf_counter()
        parse_resource_once.cache_clear()
        df = prepare_dataframe(raw_df)
        print(f"Prepare DataFrame: {time.perf_counter()-start:.2f}s")
        del raw_df
        
        start = time.perf_counter()
        ring_peaks = build_ring_peak_summary(df)
        ring_peaks["Node Count"] = 0
        print(f"Ring Summary: {time.perf_counter()-start:.2f}s")

        for idx, row in ring_peaks.iterrows():
            norm_ring = normalize_ring_name(row.get("Ring", ""))
            norm_board = normalize_board_pair(row.get("Board Pair", ""))

            nodes = set()

            mappings = RingNodeMapping.objects.filter(
                normalized_ring=norm_ring,
                normalized_board_pair=norm_board
            )

            for m in mappings:
                if m.source_ne:
                    nodes.add(m.source_ne)
                if m.sink_ne:
                    nodes.add(m.sink_ne)

            ring_peaks.at[idx, "Node Count"] = len(nodes)

        start = time.perf_counter()
        g100_peaks = build_100g_peak_summary(df)
        print(f"100G Summary: {time.perf_counter()-start:.2f}s")
        service_peaks = build_service_peak_summary(df)
        print(f"Access Service Summary: {time.perf_counter()-start:.2f}s")
        ftth_peaks = build_access_service_summary(df)
        print(f"build acces: {time.perf_counter()-start:.2f}s")

        start = time.perf_counter()
        _save_analysis_to_db(
            files,
            ring_peaks,
            g100_peaks,
            ftth_peaks
        )
        print(f"Save DB: {time.perf_counter()-start:.2f}s")
        
        start = time.perf_counter()
        _store_results(
            request,
            df,
            ring_peaks,
            g100_peaks,
            ftth_peaks
        )
        print(f"Store Cache: {time.perf_counter()-start:.2f}s")
        
        context = _build_context(
            df,
            ring_peaks,
            g100_peaks,
            service_peaks,
            errors=errors,
            request=request
        )
        

        return render(request, "dashboard/result.html", context)

    df, ring_peaks, g100_peaks, ftth_peaks = _load_results(request)

    if df is None:
        df, ring_peaks, g100_peaks, ftth_peaks = _latest_db_results()

        if df is None:
            return redirect("upload")

        context = _build_context(
            df,
            ring_peaks,
            g100_peaks,
            service_peaks=pd.DataFrame(),
            request=None
        )

        context["errors"] = [
            "Showing latest saved database result. Proof/debug requires re-upload in the current session."
        ]

        return render(request, "dashboard/result.html", context)

    context = _build_context(df, ring_peaks, g100_peaks, request=request)

    return render(request, "dashboard/result.html", context)


def weekly_trend_view(request):
    trend_type = request.GET.get("type", "ring")
    selected_item = request.GET.get("item", "")
    selected_metric = request.GET.get("metric", "peak")

    metric_map_ring = {
        "peak": "total_tx_gbps",
        "average": "total_avg_gbps",
        "par": "peak_average_ratio",
        "util": "util_percent",
    }

    metric_map_100g = {
        "peak": "peak_util_gbps",
        "average": "average_util_gbps",
        "par": "peak_average_ratio",
    }

    metric_label_map = {
        "peak": "Peak",
        "average": "Average",
        "par": "Peak Average Ratio",
        "util": "Util %",
    }

    ring_options = [
        {
            "value": item,
            "label": item,
        }
        for item in RingSummary.objects.values_list("ring", flat=True)
        .distinct()
        .order_by("ring")
    ]

    g100_options = [
        {
            "value": item,
            "label": item,
        }
        for item in Link100GSummary.objects.values_list("link_name", flat=True)
        .distinct()
        .order_by("link_name")
    ]

    if trend_type == "g100":
        items = [opt["value"] for opt in g100_options]

        if not selected_item and items:
            selected_item = items[0]

        qs = Link100GSummary.objects.filter(
            link_name=selected_item
        ).select_related("upload_run")

        metric_field = metric_map_100g.get(selected_metric, "peak_util_gbps")

    else:
        trend_type = "ring"
        items = [opt["value"] for opt in ring_options]

        if not selected_item and items:
            selected_item = items[0]

        qs = RingSummary.objects.filter(
            ring=selected_item
        ).select_related("upload_run")

        metric_field = metric_map_ring.get(selected_metric, "total_tx_gbps")

    weekly_data = defaultdict(list)
    
    for row in qs:
        raw_peak_time = str(row.peak_time).strip().strip("'").strip('"')
        peak_time = pd.to_datetime(raw_peak_time, errors="coerce")

        if pd.isna(peak_time):
            #print("BAD PEAK TIME:", repr(row.peak_time))
            continue
        iso_year, iso_week, _ = peak_time.isocalendar()
        week_label = f"{iso_year}-W{iso_week:02d}"
        value = getattr(row, metric_field, 0) or 0
        #print("PARSED:", peak_time, "WEEK:", week_label, "VALUE:", value)
        weekly_data[week_label].append(float(value))

    labels = []
    values = []

    for week in sorted(weekly_data.keys()):
        labels.append(week)
        values.append(round(max(weekly_data[week]), 3))

    metric_label = metric_label_map.get(selected_metric, "Peak")

    if trend_type == "g100":
        chart_title = f"100G Weekly Trend - {selected_item}"
    else:
        chart_title = f"Ring Weekly Trend - {selected_item}"

    context = {
        "selected_type": trend_type,
        "selected_item": selected_item,
        "selected_metric": selected_metric,

        "ring_options": ring_options,
        "g100_options": g100_options,

        "chart_labels_json": json.dumps(labels),
        "chart_values_json": json.dumps(values),
        "has_trend_data": bool(labels and values),

        "chart_title": chart_title,
        "metric_label": metric_label,
    }

    #print("Trend type:", trend_type)
   # print("Selected item:", selected_item)
   # print("Items:", items[:5])
   # print("Labels:", labels)
   # print("Values:", values)
   # print("RAW PEAK TIME:", repr(row.peak_time))
    return render(request, "dashboard/weekly_trend.html", context)

def proof_data_view(request):
    df, ring_peaks, g100_peaks, ftth_peaks = _load_results(request)

    if df is None:
        return JsonResponse(
            {"ok": False, "error": "No cached upload data found. Please upload files again."},
            status=400
        )

    proof_context = _proof_context(df, ring_peaks, g100_peaks, request=request)
    html = render_to_string("dashboard/proof_content.html", proof_context, request=request)

    return JsonResponse({"ok": True, "html": html})

def top10_ring_weekly_trend():
    qs = RingSummary.objects.all()

    weekly_ring_data = defaultdict(list)

    for row in qs:
        raw_peak_time = str(row.peak_time).strip().strip("'").strip('"')
        peak_time = pd.to_datetime(raw_peak_time, errors="coerce")
        if pd.isna(peak_time):
            continue

        iso_year, iso_week, _ = peak_time.isocalendar()

        week_label = f"{iso_year}-W{iso_week:02d}"

        key = (week_label, row.ring)

        weekly_ring_data[key].append(
            float(row.total_tx_gbps or 0)
        )

    weekly_top = {}

    weeks = sorted(
        set([week for week, ring in weekly_ring_data.keys()])
    )

    for week in weeks:

        ring_values = []

        for (w, ring), values in weekly_ring_data.items():

            if w == week:

                ring_values.append({
                    "ring": ring,
                    "value": round(max(values), 3)
                })

        ring_values = sorted(
            ring_values,
            key=lambda x: x["value"],
            reverse=True
        )[:10]

        weekly_top[week] = ring_values
        
    return weekly_top

def top10_ring_weekly_chart_data():
    weekly_top = top10_ring_weekly_trend()

    if not weekly_top:
        return {
            "labels": [],
            "datasets": []
        }

    # Show only recent weeks to keep chart clean
    weeks = sorted(weekly_top.keys())[-8:]

    if not weeks:
        return {
            "labels": [],
            "datasets": []
        }

    latest_week = weeks[-1]

    # Take ONLY latest week's Top 10 rings
    latest_rings = [
        row["ring"]
        for row in weekly_top.get(latest_week, [])
    ][:10]

    datasets = []

    for ring in latest_rings:
        ring_data = []

        for week in weeks:
            value = 0

            # Only use value if this ring appeared in that week's Top 10
            for row in weekly_top.get(week, []):
                if row["ring"] == ring:
                    value = row["value"]
                    break

            ring_data.append(value)

        datasets.append({
            "label": ring,
            "data": ring_data,
            "fill": False,
            "borderWidth": 2,
            "pointRadius": 4,
            "tension": 0.3,
        })

    return {
        "labels": weeks,
        "datasets": datasets
    }
    
def top10_current_table_ring_chart_data(top10_ring_rows):
    rings = [row["Ring"] for row in top10_ring_rows]

    weekly_data = defaultdict(lambda: defaultdict(list))

    qs = RingSummary.objects.filter(ring__in=rings)

    for row in qs:
        raw_peak_time = str(row.peak_time).strip().strip("'").strip('"')
        peak_time = pd.to_datetime(raw_peak_time, errors="coerce")

        if pd.isna(peak_time):
            continue

        iso_year, iso_week, _ = peak_time.isocalendar()
        week = f"{iso_year}-W{iso_week:02d}"

        weekly_data[row.ring][week].append(float(row.total_tx_gbps or 0))

    weeks = sorted({
        week
        for ring_data in weekly_data.values()
        for week in ring_data.keys()
    })[-8:]

    datasets = []

    for ring in rings:
        data = []

        for week in weeks:
            values = weekly_data.get(ring, {}).get(week, [])
            data.append(round(max(values), 3) if values else None)

        datasets.append({
            "label": ring,
            "data": data,
            "fill": False,
            "borderWidth": 2,
            "pointRadius": 4,
            "tension": 0.3,
        })

    return {
        "labels": weeks,
        "datasets": datasets,
    }

def download_excel_view(request):
    df, ring_peaks, g100_peaks, ftth_peaks = _load_results(request)

    if df is None:
        df, ring_peaks, g100_peaks, ftth_peaks = _latest_db_results()

        if df is None:
            return redirect("upload")

        service_peaks = pd.DataFrame()
    else:
        service_peaks = build_service_peak_summary(df)

    ring_node_details = build_ring_node_detail(ring_peaks)

    excel_bytes = to_excel_bytes(
        ring_peaks,
        g100_peaks,
        service_peaks,
        ftth_peaks,
        ring_node_details
    )

    response = HttpResponse(
        excel_bytes,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    response["Content-Disposition"] = 'attachment; filename="ring_100g_service_summary.xlsx"'

    return response

def ring_trend_api(request):

    ring = request.GET.get("ring","")

    qs = RingSummary.objects.filter(
        ring=ring
    )

    weekly_data=defaultdict(list)

    for row in qs:

        raw_peak=str(row.peak_time)

        peak_time=pd.to_datetime(
            raw_peak,
            errors="coerce"
        )

        if pd.isna(peak_time):
            continue

        iso_year,iso_week,_=peak_time.isocalendar()

        week=f"{iso_year}-W{iso_week:02d}"

        weekly_data[week].append(
            float(
                row.total_tx_gbps or 0
            )
        )

    labels=[]
    values=[]

    for week in sorted(weekly_data):

        labels.append(week)

        values.append(
            round(
                max(weekly_data[week]),
                3
            )
        )

    return JsonResponse({
        "labels":labels,
        "values":values
    })

####new added 23rd may
def ring_nodes_view(request):
    ring = request.GET.get("ring", "")
    board_pair = request.GET.get("board_pair", "")

    norm_ring = normalize_ring_name(ring)
    norm_board = normalize_board_pair(board_pair)

    rows = RingNodeMapping.objects.filter(
        normalized_ring=norm_ring,
        normalized_board_pair=norm_board
    ).values("source_ne", "sink_ne")

    return JsonResponse({
        "ring": ring,
        "board_pair": board_pair,
        "rows": list(rows),
    })


def build_ring_node_detail(ring_peaks):
    rows = []

    if ring_peaks is None or ring_peaks.empty:
        return pd.DataFrame(
            rows,
            columns=["Ring", "Board Pair", "Source NE", "Sink NE"]
        )

    for _, row in ring_peaks.iterrows():
        ring = str(row.get("Ring", ""))
        board_pair = str(row.get("Board Pair", ""))

        norm_ring = normalize_ring_name(ring)
        norm_board = normalize_board_pair(board_pair)

        mappings = RingNodeMapping.objects.filter(
            normalized_ring=norm_ring,
            normalized_board_pair=norm_board
        )

        for m in mappings:
            rows.append({
                "Ring": ring,
                "Board Pair": board_pair,
                "Source NE": m.source_ne,
                "Sink NE": m.sink_ne,
            })

    return pd.DataFrame(
        rows,
        columns=["Ring", "Board Pair", "Source NE", "Sink NE"]
    )
