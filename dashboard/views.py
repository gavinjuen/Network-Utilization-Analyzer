from __future__ import annotations

from pathlib import Path
from uuid import uuid4
import time

from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.template.loader import render_to_string
import pandas as pd

from .forms import UploadFilesForm
from .calculations import (
    read_uploaded_files, prepare_dataframe, build_ring_peak_summary,
    build_100g_peak_summary, build_ring_proof, build_100g_proof, to_excel_bytes
)

CACHE_MAX_AGE_SECONDS = 60 * 60 * 12


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


def _store_results(request, df: pd.DataFrame, ring_peaks: pd.DataFrame, g100_peaks: pd.DataFrame) -> None:
    _purge_old_cache_files()

    old_cache_id = request.session.get("cache_id")
    if old_cache_id:
        for suffix in ("df", "ring", "g100"):
            try:
                _cache_file(old_cache_id, suffix).unlink(missing_ok=True)
            except OSError:
                pass

    cache_id = uuid4().hex
    df.to_pickle(_cache_file(cache_id, "df"), compression="gzip")
    ring_peaks.to_pickle(_cache_file(cache_id, "ring"), compression="gzip")
    g100_peaks.to_pickle(_cache_file(cache_id, "g100"), compression="gzip")

    request.session["cache_id"] = cache_id
    request.session.modified = True


def _load_results(request):
    cache_id = request.session.get("cache_id")
    if not cache_id:
        return None, None, None

    df_path = _cache_file(cache_id, "df")
    ring_path = _cache_file(cache_id, "ring")
    g100_path = _cache_file(cache_id, "g100")

    if not df_path.exists() or not ring_path.exists() or not g100_path.exists():
        return None, None, None

    try:
        df = pd.read_pickle(df_path, compression="gzip")
        ring_peaks = pd.read_pickle(ring_path, compression="gzip")
        g100_peaks = pd.read_pickle(g100_path, compression="gzip")
    except Exception:
        return None, None, None

    return df, ring_peaks, g100_peaks


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
            options.append({"label": label, "ring": str(row["Ring"]), "board_pair": board_pair, "instance": instance})
        context["ring_debug_options"] = options

    if not g100_peaks.empty:
        context["g100_debug_options"] = [str(v) for v in g100_peaks["100G Link"].dropna().tolist()]

    if request is not None:
        debug_type = request.GET.get("debug", "ring")
        context["selected_debug_type"] = debug_type

        if debug_type == "g100" and context["g100_debug_options"]:
            selected_link = request.GET.get("g100_link") or context["g100_debug_options"][0]
            context["selected_g100_link"] = selected_link
            proof_100g = build_100g_proof(df, selected_link)
            if not proof_100g.empty:
                proof_100g = proof_100g.fillna("")
                cols = ["Collection Time", "100G Link", "TX (Gbps)", "RX (Gbps)", "Selected Max TX/RX (Gbps)", "Resource Name", "Source File"]
                context["proof_g100_columns"] = cols
                context["proof_g100_rows"] = proof_100g[cols].to_dict(orient="records")
        else:
            context["selected_debug_type"] = "ring"
            if context["ring_debug_options"]:
                selected_label = request.GET.get("ring_label") or context["ring_debug_options"][0]["label"]
                context["selected_ring_label"] = selected_label
                match = next((o for o in context["ring_debug_options"] if o["label"] == selected_label), context["ring_debug_options"][0])
                endpoint_totals, same_time, timestamp_totals = build_ring_proof(df, match["ring"], match.get("board_pair", ""), match["instance"])
                if not endpoint_totals.empty:
                    endpoint_totals = endpoint_totals.fillna("")
                    same_time = same_time.fillna("")
                    timestamp_totals = timestamp_totals.fillna("")
                    context["proof_ring_endpoint_columns"] = ["Collection Time", "Endpoint", "TX (Gbps)"]
                    context["proof_ring_endpoint_rows"] = endpoint_totals[context["proof_ring_endpoint_columns"]].to_dict(orient="records")
                    context["proof_ring_timestamp_columns"] = ["Collection Time", "Total TX (Gbps)"]
                    context["proof_ring_timestamp_rows"] = timestamp_totals[context["proof_ring_timestamp_columns"]].to_dict(orient="records")
                    context["proof_ring_same_time_columns"] = ["Collection Time", "Endpoint", "TX (Gbps)"]
                    context["proof_ring_same_time_rows"] = same_time[context["proof_ring_same_time_columns"]].to_dict(orient="records")
                    try:
                        context["proof_ring_step3_total"] = round(float(same_time["TX (Gbps)"].sum()), 3)
                    except Exception:
                        context["proof_ring_step3_total"] = None
    return context


def _build_context(df, ring_peaks, g100_peaks, errors=None, request=None):
    congested_ring_name = ""
    congested_ring_value = 0.0
    if not ring_peaks.empty:
        congested_ring_row = ring_peaks.sort_values(["Total TX (Gbps)", "Util %"], ascending=[False, False]).iloc[0]
        congested_ring_name = str(congested_ring_row["Ring"])
        congested_ring_value = float(congested_ring_row["Total TX (Gbps)"])

    congested_100g_name = ""
    congested_100g_value = 0.0
    if not g100_peaks.empty:
        congested_100g_row = g100_peaks.sort_values(["Peak Util (Gbps)"], ascending=[False]).iloc[0]
        congested_100g_name = str(congested_100g_row["100G Link"])
        congested_100g_value = float(congested_100g_row["Peak Util (Gbps)"])

    context = {
        "errors": errors or [],
        "ring_columns": list(ring_peaks.columns),
        "ring_rows": ring_peaks.fillna("").to_dict(orient="records"),
        "g100_columns": list(g100_peaks.columns),
        "g100_rows": g100_peaks.fillna("").to_dict(orient="records"),
        "ring_count": ring_peaks["Ring"].nunique() if not ring_peaks.empty else 0,
        "g100_count": len(g100_peaks),
        "congested_ring_name": congested_ring_name,
        "congested_ring_value": congested_ring_value,
        "congested_100g_name": congested_100g_name,
        "congested_100g_value": congested_100g_value,
        "top10_ring_columns": ["Ring", "Board Pair", "Link Instance", "Total TX (Gbps)", "Util %", "Util Band"],
        "top10_ring_rows": [],
        "top10_100g_columns": ["100G Link", "Peak Util (Gbps)", "Util Band", "Peak Time"],
        "top10_100g_rows": [],
        "top10_ring_chart_labels": [],
        "top10_ring_chart_values": [],
        "top10_100g_chart_labels": [],
        "top10_100g_chart_values": [],
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

    context.update(_proof_context(df, ring_peaks, g100_peaks, request=request))
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

        df = prepare_dataframe(raw_df)
        del raw_df

        ring_peaks = build_ring_peak_summary(df)
        g100_peaks = build_100g_peak_summary(df)

        _store_results(request, df, ring_peaks, g100_peaks)
        context = _build_context(df, ring_peaks, g100_peaks, errors=errors, request=request)
        return render(request, "dashboard/result.html", context)

    df, ring_peaks, g100_peaks = _load_results(request)
    if df is None:
        return redirect("upload")
    context = _build_context(df, ring_peaks, g100_peaks, request=request)
    return render(request, "dashboard/result.html", context)


def proof_data_view(request):
    df, ring_peaks, g100_peaks = _load_results(request)
    if df is None:
        return JsonResponse({"ok": False, "error": "No cached upload data found. Please upload files again."}, status=400)

    proof_context = _proof_context(df, ring_peaks, g100_peaks, request=request)
    html = render_to_string("dashboard/proof_content.html", proof_context, request=request)
    return JsonResponse({"ok": True, "html": html})


def download_excel_view(request):
    df, ring_peaks, g100_peaks = _load_results(request)
    if df is None:
        return redirect("upload")
    excel_bytes = to_excel_bytes(ring_peaks, g100_peaks)
    response = HttpResponse(
        excel_bytes,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="ring_100g_summary.xlsx"'
    return response
