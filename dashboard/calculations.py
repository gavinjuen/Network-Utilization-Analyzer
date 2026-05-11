import io
import os
import re
import zipfile
import pandas as pd

REQUIRED_COLUMNS = [
    "Resource Name",
    "Collection Time",
    "Granularity",
    "RXBPS(bit/s)",
    "TXBPS(bit/s)",
]

def parse_bps(value):
    if pd.isna(value):
        return None
    s = str(value).strip().upper().replace(",", "")
    s = s.replace("BIT/S", "").replace("BPS", "").strip()
    match = re.search(r"([0-9]*\.?[0-9]+)\s*([KMG]?)", s)
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2)
    multiplier = {"": 1.0, "K": 1e3, "M": 1e6, "G": 1e9}[unit]
    return number * multiplier

def ring_from_resource(resource_name):
    if not resource_name:
        return ""
    m = re.search(r"(\[RING_[^\]]+\])", str(resource_name))
    return m.group(1) if m else ""

def extract_endpoint(resource_name):
    if not resource_name:
        return ""
    text = str(resource_name).strip()
    if "-MAC" in text:
        return text.split("-MAC")[0].strip()
    return text

def extract_source_site(resource_name):
    return extract_endpoint(resource_name)

def extract_sink_site(resource_name):
    if not resource_name:
        return ""
    text = str(resource_name)
    upper_text = text.upper()
    marker = "[100G LINK] TO"
    start = upper_text.find(marker)
    if start == -1:
        return ""
    sink_part = text[start + len(marker):].strip()
    mac_pos = sink_part.upper().find("-MAC")
    if mac_pos != -1:
        sink_part = sink_part[:mac_pos].strip()
    return sink_part.strip().strip(" )-")

def extract_100g_link_name(resource_name):
    source = extract_source_site(resource_name)
    sink = extract_sink_site(resource_name)
    if not source or not sink:
        return ""
    return f"{source} -> {sink}"

def util_band_ring(value_gbps):
    if value_gbps is None or pd.isna(value_gbps):
        return "No Data"
    v = float(value_gbps)
    if v >= 8.0:
        return "Critical"
    elif v >= 5.0:
        return "Warning"
    return "Normal"

def util_band_100g(value_gbps):
    if value_gbps is None or pd.isna(value_gbps):
        return "No Data"
    v = float(value_gbps)
    if v >= 70.0:
        return "Critical"
    elif v >= 40.0:
        return "Warning"
    return "Normal"

def detect_board_type(resource_name):
    text = str(resource_name).upper()
    if "UNS4MP" in text:
        return "UNS4MP"
    if "UNQ2" in text:
        return "UNQ2"
    if "U220" in text:
        return "U220"
    if "U402" in text:
        return "U402"
    if "EX10" in text:
        return "EX10"
    if "E224" in text:
        return "E224"
    if "EX2" in text:
        return "EX2"
    if "EM20" in text:
        return "EM20"
    return "OTHER"

def extract_link_instance(resource_name):
    text = str(resource_name).upper()
    m = re.search(r"(?:UNQ2|U220)-(\d+)", text)
    return m.group(1) if m else ""

def extract_service_group(resource_name):
    text = str(resource_name).upper()

    has_digi_celcom = ("DIGI" in text) or ("CELCOM" in text)
    has_maxis = ("MAXIS" in text)
    has_ytl_um = ("YTL" in text) or bool(re.search(r"\bUM\b", text)) or ("UMOBILE" in text) or ("U MOBILE" in text)
    has_olt_dnb = ("OLT" in text) or ("DNB" in text)

    # Priority order requested by user:
    # Digi/Celcom > Maxis > YTL/UM > OLT/DNB
    if has_digi_celcom:
        return "Digi/Celcom"
    if has_maxis:
        return "Maxis"
    if has_ytl_um:
        return "YTL/UM"
    if has_olt_dnb:
        return "OLT/DNB"
    return ""

def read_csv_bytes(raw_bytes, source_file, skiprows):
    last_error = None
    read_attempts = [
        {"sep": None, "engine": "python", "encoding": "utf-8-sig"},
        {"sep": ";", "engine": "python", "encoding": "utf-8-sig"},
        {"sep": ",", "engine": "python", "encoding": "utf-8-sig"},
        {"sep": None, "engine": "python", "encoding": "latin1"},
        {"sep": ";", "engine": "python", "encoding": "latin1"},
        {"sep": ",", "engine": "python", "encoding": "latin1"},
    ]
    for opts in read_attempts:
        try:
            df = pd.read_csv(io.BytesIO(raw_bytes), skiprows=skiprows, on_bad_lines="skip", **opts)
            df.columns = df.columns.str.strip()
            missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
            if missing:
                last_error = f"Missing columns: {missing}"
                continue
            df = df[REQUIRED_COLUMNS].copy()
            df["Source File"] = source_file
            return df
        except Exception as e:
            last_error = str(e)
    raise ValueError(f"Failed to read {source_file}. Last error: {last_error}")

def read_uploaded_files(uploaded_files, skiprows):
    dfs, errors = [], []
    for uploaded_file in uploaded_files:
        filename = uploaded_file.name
        try:
            if filename.lower().endswith(".zip"):
                zip_bytes = uploaded_file.read()
                with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
                    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv") and not n.startswith("__MACOSX/")]
                    if not csv_names:
                        errors.append(f"{filename}: No CSV files found inside ZIP.")
                        continue
                    for csv_name in csv_names:
                        try:
                            raw = zf.read(csv_name)
                            df = read_csv_bytes(raw, os.path.basename(csv_name), skiprows)
                            dfs.append(df)
                        except Exception as e:
                            errors.append(f"{csv_name}: {e}")
            elif filename.lower().endswith(".csv"):
                raw = uploaded_file.read()
                df = read_csv_bytes(raw, filename, skiprows)
                dfs.append(df)
        except Exception as e:
            errors.append(f"{filename}: {e}")
    if not dfs:
        return pd.DataFrame(), errors
    combined = pd.concat(dfs, ignore_index=True)
    combined.drop_duplicates(inplace=True)
    return combined, errors

def prepare_dataframe(combined):
    combined = combined.copy()
    combined["Collection Time"] = pd.to_datetime(combined["Collection Time"], dayfirst=True, errors="coerce")
    combined["TX_bps"] = combined["TXBPS(bit/s)"].apply(parse_bps)
    combined["RX_bps"] = combined["RXBPS(bit/s)"].apply(parse_bps)
    combined["MAX_bps"] = combined[["TX_bps", "RX_bps"]].max(axis=1)
    combined["Ring"] = combined["Resource Name"].apply(ring_from_resource)
    combined["Endpoint"] = combined["Resource Name"].apply(extract_endpoint)
    combined["Source Site"] = combined["Resource Name"].apply(extract_source_site)
    combined["Sink Site"] = combined["Resource Name"].apply(extract_sink_site)
    combined["100G Link"] = combined["Resource Name"].apply(extract_100g_link_name)
    combined["Board Type"] = combined["Resource Name"].apply(detect_board_type)
    combined["Link Instance"] = combined["Resource Name"].apply(extract_link_instance)
    combined["Service Group"] = combined["Resource Name"].apply(extract_service_group)
    return combined

def get_board_pair_label(group_df):
    board_types = sorted(set(group_df["Board Type"].dropna().astype(str)))
    board_types = [b for b in board_types if b and b != "OTHER"]
    if not board_types:
        return "OTHER"
    board_set = set(board_types)
    # Only merge known mixed pairings that are intended to be one logical ring.
    if board_set == {"E224", "EX10"}:
        return "E224/EX10"
    if board_set == {"U402", "UNS4MP"}:
        return "U402/UNS4MP"
    if board_set == {"EX2", "EM20"}:
        return "EX2/EM20"
    # Otherwise keep a single board type label when possible.
    if len(board_types) == 1:
        return board_types[0]
    # For unexpected mixed groups, keep them visible instead of hiding them.
    return "/".join(board_types)

def calculate_group_capacity(group_df):
    board_types = set(group_df["Board Type"].dropna().astype(str))
    board_types = {b for b in board_types if b and b != "OTHER"}
    if board_types.issubset({"UNQ2", "U220"}) and len(board_types) > 0:
        instance_count = group_df.loc[group_df["Link Instance"] != "", "Link Instance"].nunique()
        return float(max(instance_count * 10, 10))
    capacities = []
    if "EX10" in board_types:
        capacities.append(10)
    if "E224" in board_types:
        capacities.append(10)
    if "UNS4MP" in board_types:
        capacities.append(20)
    if "UNQ2" in board_types or "U220" in board_types:
        capacities.append(10)

    return float(min(capacities) if capacities else 10)

def build_ring_peak_summary(df):
    ring_df = df[df["Ring"] != ""].dropna(subset=["Collection Time", "TX_bps"]).copy()
    output_columns = [
        "Ring",
        "Board Pair",
        "Link Instance",
        "Peak Time",
        "Endpoint 1",
        "TX 1 (Gbps)",
        "Endpoint 2",
        "TX 2 (Gbps)",
        "Total TX (Gbps)",
        "Avg Endpoint 1 (Gbps)",
        "Avg Endpoint 2 (Gbps)",
        "Total Avg (Gbps)",
        "Peak Average Ratio",
        "Max Capacity (Gbps)",
        "Util %",
        "Util Band",
    ]
    if ring_df.empty:
        return pd.DataFrame(columns=output_columns)

    result_rows = []

    link_df = ring_df[(ring_df["Board Type"].isin(["UNQ2", "U220"])) & (ring_df["Link Instance"] != "")].copy()
    if not link_df.empty:
        # Final U220/UNQ2 rules:
        # - YTL/UM/OLT/DNB -> merge only with same instance
        # - Digi/Celcom/Maxis -> merge only with same instance
        # - YTL/UM -> merge across different instances
        # - OLT/DNB -> merge across different instances
        # - Digi/Celcom -> merge across different instances
        # - Maxis -> merge across different instances

        def u220_monitor_key(row):
            ring = str(row["Ring"])
            instance = str(row["Link Instance"])
            service = str(row["Service Group"])
            same_instance_only = {"YTL/UM/OLT/DNB", "Digi/Celcom/Maxis"}
            if service in same_instance_only:
                return f"{ring}|SERVICE|{service}|INSTANCE|{instance}"
            return f"{ring}|SERVICE|{service}"

        link_df = link_df[link_df["Service Group"].astype(str) != ""].copy()
        if not link_df.empty:
            link_df["Monitor Key"] = link_df.apply(u220_monitor_key, axis=1)
            link_df["Board Pair"] = "U220/UNQ2 (" + link_df["Service Group"].astype(str) + ")"

            endpoint_time_totals = link_df.groupby(
                ["Ring", "Monitor Key", "Service Group", "Collection Time", "Endpoint"],
                as_index=False
            )["TX_bps"].sum()

            for (ring, monitor_key, service_group), ring_grp in endpoint_time_totals.groupby(
                ["Ring", "Monitor Key", "Service Group"]
            ):
                raw_group = link_df[(link_df["Ring"] == ring) & (link_df["Monitor Key"] == monitor_key)].copy()
                timestamp_totals = ring_grp.groupby("Collection Time", as_index=False)["TX_bps"].sum().rename(
                    columns={"TX_bps": "Total_TX_bps"}
                )
                if timestamp_totals.empty:
                    continue

                peak_time = timestamp_totals.loc[timestamp_totals["Total_TX_bps"].idxmax(), "Collection Time"]
                same_time_grp = (
                    ring_grp[ring_grp["Collection Time"] == peak_time]
                    .copy()
                    .sort_values("TX_bps", ascending=False)
                    .reset_index(drop=True)
                )

                ep1 = same_time_grp.iloc[0]["Endpoint"] if len(same_time_grp) >= 1 else ""
                tx1 = float(same_time_grp.iloc[0]["TX_bps"]) if len(same_time_grp) >= 1 else 0.0
                ep2 = same_time_grp.iloc[1]["Endpoint"] if len(same_time_grp) >= 2 else ""
                tx2 = float(same_time_grp.iloc[1]["TX_bps"]) if len(same_time_grp) >= 2 else 0.0
                total = tx1 + tx2

                endpoint_means = ring_grp.groupby("Endpoint", as_index=False)["TX_bps"].mean()
                avg_ep1_bps = float(
                    endpoint_means.loc[endpoint_means["Endpoint"] == ep1, "TX_bps"].iloc[0]
                ) if ep1 and (endpoint_means["Endpoint"] == ep1).any() else 0.0
                avg_ep2_bps = float(
                    endpoint_means.loc[endpoint_means["Endpoint"] == ep2, "TX_bps"].iloc[0]
                ) if ep2 and (endpoint_means["Endpoint"] == ep2).any() else 0.0
                avg_total_bps = avg_ep1_bps + avg_ep2_bps
                peak_avg_ratio = (total / avg_total_bps) if avg_total_bps > 0 else 0.0

                instance_list = sorted(
                    {str(v) for v in raw_group["Link Instance"].dropna().astype(str) if str(v).strip()}
                )
                display_instance = "/".join(instance_list)

                result_rows.append({
                    "Ring": ring,
                    "Board Pair": f"U220/UNQ2 ({service_group})",
                    "Link Instance": display_instance,
                    "Peak Time": peak_time,
                    "Endpoint 1": ep1,
                    "TX 1 (Gbps)": round(tx1 / 1e9, 3),
                    "Endpoint 2": ep2,
                    "TX 2 (Gbps)": round(tx2 / 1e9, 3),
                    "Total TX (Gbps)": round(total / 1e9, 3),
                    "Avg Endpoint 1 (Gbps)": round(avg_ep1_bps / 1e9, 3),
                    "Avg Endpoint 2 (Gbps)": round(avg_ep2_bps / 1e9, 3),
                    "Total Avg (Gbps)": round(avg_total_bps / 1e9, 3),
                    "Peak Average Ratio": round(peak_avg_ratio, 2),
                    "Max Capacity (Gbps)": calculate_group_capacity(raw_group),
                })

    non_link_df = ring_df[(~ring_df["Board Type"].isin(["UNQ2", "U220"]))].copy()
    if not non_link_df.empty:
        # Selective pairing rule:
        # - E224 and EX10 under the same ring are merged into one logical board pair E224/EX10
        # - other board types such as UNS4MP stay separate even if the ring name is the same
        def assign_non_link_group(row, ring_type_map):
            board_type = str(row["Board Type"])
            ring = row["Ring"]
            ring_types = ring_type_map.get(ring, set())
            if board_type in {"E224", "EX10"} and ring_types.issuperset({"E224", "EX10"}):
                return "E224/EX10"
            if board_type in {"U402", "UNS4MP"} and ring_types.issuperset({"U402", "UNS4MP"}):
                return "U402/UNS4MP"
            if board_type in {"EX2", "EM20"} and ring_types.issuperset({"EX2", "EM20"}):
                return "EX2/EM20"
            return board_type

        ring_type_map = (
            non_link_df.groupby("Ring")["Board Type"]
            .apply(lambda s: {str(v) for v in s.dropna().astype(str) if v and v != "OTHER"})
            .to_dict()
        )
        non_link_df["Board Pair"] = non_link_df.apply(lambda row: assign_non_link_group(row, ring_type_map), axis=1)

        endpoint_time_totals = non_link_df.groupby(
            ["Ring", "Board Pair", "Collection Time", "Endpoint"],
            as_index=False
        )["TX_bps"].sum()

        for (ring, board_pair), ring_grp in endpoint_time_totals.groupby(["Ring", "Board Pair"]):
            raw_group = non_link_df[
                (non_link_df["Ring"] == ring) & (non_link_df["Board Pair"] == board_pair)
            ].copy()

            timestamp_totals = ring_grp.groupby("Collection Time", as_index=False)["TX_bps"].sum().rename(
                columns={"TX_bps": "Total_TX_bps"}
            )
            if timestamp_totals.empty:
                continue

            peak_time = timestamp_totals.loc[timestamp_totals["Total_TX_bps"].idxmax(), "Collection Time"]
            same_time_grp = (
                ring_grp[ring_grp["Collection Time"] == peak_time]
                .copy()
                .sort_values("TX_bps", ascending=False)
                .reset_index(drop=True)
            )

            ep1 = same_time_grp.iloc[0]["Endpoint"] if len(same_time_grp) >= 1 else ""
            tx1 = float(same_time_grp.iloc[0]["TX_bps"]) if len(same_time_grp) >= 1 else 0.0
            ep2 = same_time_grp.iloc[1]["Endpoint"] if len(same_time_grp) >= 2 else ""
            tx2 = float(same_time_grp.iloc[1]["TX_bps"]) if len(same_time_grp) >= 2 else 0.0

            if ring == "[RING_LHDT03]":
                ep1_rows = ring_grp[ring_grp["Endpoint"] == ep1].copy()
                ep2_rows = ring_grp[ring_grp["Endpoint"] == ep2].copy()

                print("=" * 100)
                print("RING DEBUG")
                print("Ring:", ring)
                print("Peak Time:", peak_time)
                print("Endpoint 1:", ep1)
                print("Endpoint 2:", ep2)
                print("TX 1 (Gbps):", round(tx1 / 1e9, 3))
                print("TX 2 (Gbps):", round(tx2 / 1e9, 3))
                print("Total TX (Gbps):", round((tx1 + tx2) / 1e9, 3))

                print("\nEndpoint 1 rows:")
                print(ep1_rows[["Collection Time", "Endpoint", "TX_bps"]].to_string(index=False))

                print("\nEndpoint 2 rows:")
                print(ep2_rows[["Collection Time", "Endpoint", "TX_bps"]].to_string(index=False))

                avg_ep1 = ep1_rows["TX_bps"].mean() / 1e9 if not ep1_rows.empty else 0
                avg_ep2 = ep2_rows["TX_bps"].mean() / 1e9 if not ep2_rows.empty else 0
                final_avg = avg_ep1 + avg_ep2

                print("\nAverage check:")
                print("Avg Endpoint 1 (Gbps):", round(avg_ep1, 3))
                print("Avg Endpoint 2 (Gbps):", round(avg_ep2, 3))
                print("Final Average (Gbps):", round(final_avg, 3))

                print("\nPeak Average Ratio check:")
                par = ((tx1 + tx2) / 1e9) / final_avg if final_avg > 0 else 0
                print("PAR:", round(par, 2))
                print("=" * 100)

            total = tx1 + tx2
            endpoint_means = ring_grp.groupby("Endpoint", as_index=False)["TX_bps"].mean()
            avg_ep1_bps = float(
                endpoint_means.loc[endpoint_means["Endpoint"] == ep1, "TX_bps"].iloc[0]
            ) if ep1 and (endpoint_means["Endpoint"] == ep1).any() else 0.0
            avg_ep2_bps = float(
                endpoint_means.loc[endpoint_means["Endpoint"] == ep2, "TX_bps"].iloc[0]
            ) if ep2 and (endpoint_means["Endpoint"] == ep2).any() else 0.0
            avg_total_bps = avg_ep1_bps + avg_ep2_bps
            peak_avg_ratio = (total / avg_total_bps) if avg_total_bps > 0 else 0.0

            result_rows.append({
                "Ring": ring,
                "Board Pair": board_pair,
                "Link Instance": "",
                "Peak Time": peak_time,
                "Endpoint 1": ep1,
                "TX 1 (Gbps)": round(tx1 / 1e9, 3),
                "Endpoint 2": ep2,
                "TX 2 (Gbps)": round(tx2 / 1e9, 3),
                "Total TX (Gbps)": round(total / 1e9, 3),
                "Avg Endpoint 1 (Gbps)": round(avg_ep1_bps / 1e9, 3),
                "Avg Endpoint 2 (Gbps)": round(avg_ep2_bps / 1e9, 3),
                "Total Avg (Gbps)": round(avg_total_bps / 1e9, 3),
                "Peak Average Ratio": round(peak_avg_ratio, 2),
                "Max Capacity (Gbps)": calculate_group_capacity(raw_group),
            })

    if not result_rows:
        return pd.DataFrame(columns=output_columns)

    peaks = pd.DataFrame(result_rows)
    peaks["Util %"] = (peaks["Total TX (Gbps)"] / peaks["Max Capacity (Gbps)"] * 100).round(1)
    peaks["Util Band"] = peaks["Total TX (Gbps)"].apply(util_band_ring)

    return peaks[output_columns].sort_values(
        ["Total TX (Gbps)", "Ring", "Board Pair"],
        ascending=[False, True, True]
    ).reset_index(drop=True)
    
def build_100g_peak_summary(df):
    g100_df = df[df["100G Link"] != ""].dropna(subset=["Collection Time", "MAX_bps"]).copy()
    if g100_df.empty:
        return pd.DataFrame(columns=["100G Link", "Source Site", "Sink Site", "Peak Time", "Peak Util (Gbps)", "Util Band"])
    grouped = g100_df.groupby(["100G Link", "Source Site", "Sink Site", "Collection Time"], as_index=False)["MAX_bps"].sum()
    idx = grouped.groupby("100G Link")["MAX_bps"].idxmax()
    peaks = grouped.loc[idx].copy().sort_values("MAX_bps", ascending=False)
    peaks["Peak Util (Gbps)"] = (peaks["MAX_bps"] / 1e9).round(3)
    peaks["Util Band"] = peaks["Peak Util (Gbps)"].apply(util_band_100g)
    return peaks[["100G Link", "Source Site", "Sink Site", "Collection Time", "Peak Util (Gbps)", "Util Band"]].rename(columns={"Collection Time": "Peak Time"})

def build_ring_proof(df, ring_name, board_pair="", link_instance=""):
    ring_df = df[df["Ring"] == ring_name].dropna(subset=["Collection Time", "TX_bps"]).copy()
    if link_instance:
        ring_df = ring_df[ring_df["Link Instance"] == link_instance].copy()
    elif board_pair:
        if str(board_pair) == "E224/EX10":
            ring_df = ring_df[ring_df["Board Type"].astype(str).isin(["E224", "EX10"])].copy()
        elif str(board_pair) == "U402/UNS4MP":
            ring_df = ring_df[ring_df["Board Type"].astype(str).isin(["U402", "UNS4MP"])].copy()
        elif str(board_pair) == "EX2/EM20":
            ring_df = ring_df[ring_df["Board Type"].astype(str).isin(["EX2", "EM20"])].copy()
        elif str(board_pair).startswith("U220/UNQ2 (") and str(board_pair).endswith(")"):
            fallback_group = str(board_pair)[len("U220/UNQ2 ("):-1]
            ring_df = ring_df[
                ring_df["Board Type"].astype(str).isin(["U220", "UNQ2"]) &
                (ring_df["Service Group"].astype(str) == fallback_group)
            ].copy()
        else:
            ring_df = ring_df[ring_df["Board Type"].astype(str) == str(board_pair)].copy()
    if ring_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    endpoint_totals = ring_df.groupby(["Collection Time", "Endpoint"], as_index=False)["TX_bps"].sum()
    endpoint_totals["TX (Gbps)"] = (endpoint_totals["TX_bps"] / 1e9).round(3)
    timestamp_totals = endpoint_totals.groupby("Collection Time", as_index=False)["TX_bps"].sum().rename(columns={"TX_bps": "Total_TX_bps"})
    timestamp_totals["Total TX (Gbps)"] = (timestamp_totals["Total_TX_bps"] / 1e9).round(3)
    peak_time = timestamp_totals.loc[timestamp_totals["Total_TX_bps"].idxmax(), "Collection Time"]
    same_time = endpoint_totals[endpoint_totals["Collection Time"] == peak_time].sort_values("TX_bps", ascending=False).reset_index(drop=True)
    return endpoint_totals, same_time, timestamp_totals

def build_100g_proof(df, link_name):
    link_df = df[df["100G Link"] == link_name].dropna(subset=["Collection Time", "MAX_bps"]).copy()
    if link_df.empty:
        return pd.DataFrame()
    proof = link_df[["Collection Time", "100G Link", "Source Site", "Sink Site", "TX_bps", "RX_bps", "MAX_bps", "Resource Name", "Source File"]].copy()
    proof["TX (Gbps)"] = (proof["TX_bps"] / 1e9).round(3)
    proof["RX (Gbps)"] = (proof["RX_bps"] / 1e9).round(3)
    proof["Selected Max TX/RX (Gbps)"] = (proof["MAX_bps"] / 1e9).round(3)
    return proof.sort_values("Selected Max TX/RX (Gbps)", ascending=False)

def to_excel_bytes(ring_peaks, g100_peaks):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="dd/mm/yyyy hh:mm") as writer:
        ring_peaks.to_excel(writer, sheet_name="Ring_Peak_Summary", index=False)
        g100_peaks.to_excel(writer, sheet_name="100G_Peak_Summary", index=False)
    output.seek(0)
    return output.getvalue()
