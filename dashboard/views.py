from io import StringIO
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.template.loader import render_to_string
import pandas as pd
from .forms import UploadFilesForm
from .calculations import (
    read_uploaded_files, prepare_dataframe, build_ring_peak_summary,
    build_100g_peak_summary, build_ring_proof, build_100g_proof, to_excel_bytes
)

def _store_results(request, df, ring_peaks, g100_peaks):
    request.session["ring_peaks"] = ring_peaks.to_json(date_format="iso", orient="split")
    request.session["g100_peaks"] = g100_peaks.to_json(date_format="iso", orient="split")
    request.session["prepared_df"] = df.to_json(date_format="iso", orient="split")

def _load_results(request):
    ring_json = request.session.get("ring_peaks")
    g100_json = request.session.get("g100_peaks")
    df_json = request.session.get("prepared_df")
    if not ring_json or not g100_json or not df_json:
        return None, None, None
    ring_peaks = pd.read_json(StringIO(ring_json), orient="split")
    g100_peaks = pd.read_json(StringIO(g100_json), orient="split")
    df = pd.read_json(StringIO(df_json), orient="split")
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
        "proof_g100_columns": [],
        "proof_g100_rows": [],
    }

    if not ring_peaks.empty:
        options = []
        for _, row in ring_peaks.iterrows():
            instance = "" if pd.isna(row.get("Link Instance", "")) else str(row.get("Link Instance", ""))
            label = f"{row['Ring']} | {row['Board Pair']} | {instance if instance else 'Non-UNQ2/U220'}"
            options.append({"label": label, "ring": str(row["Ring"]), "instance": instance})
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
                endpoint_totals, same_time, timestamp_totals = build_ring_proof(df, match["ring"], match["instance"])
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
    context["proof_groups"] = [
        ("Step 1: Endpoint TX Total by Timestamp", context.get("proof_ring_endpoint_columns", []), context.get("proof_ring_endpoint_rows", [])),
        ("Step 2: Total TX by Timestamp", context.get("proof_ring_timestamp_columns", []), context.get("proof_ring_timestamp_rows", [])),
        ("Step 3: Endpoints at Selected Peak Timestamp", context.get("proof_ring_same_time_columns", []), context.get("proof_ring_same_time_rows", [])),
    ]
    return context

def _build_context(df, ring_peaks, g100_peaks, errors=None, request=None):
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
        return JsonResponse({"ok": False, "error": "No session data found. Please upload files again."}, status=400)

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
