import io
import os
import re
import zipfile
import pandas as pd
import re
from functools import lru_cache
from .models import AccessBandwidth

RING_RE = re.compile(r"(\[RING_[^\]]+\])", re.IGNORECASE)
LINK_INSTANCE_RE = re.compile(r"(?:UNQ2|U220)-(\d+)", re.IGNORECASE)
DROP_RE = re.compile(r"\b([A-Z0-9_]+)\s+DROP\s*\(?([WP])\)?", re.IGNORECASE)


REQUIRED_COLUMNS = [
    "Resource Name",
    "Collection Time",
    "Granularity",
    "RXBPS(bit/s)",
    "TXBPS(bit/s)",
]

ACCESS_SERVICE_RULES = {
    "OLT": "FTTH",
    "DIGI": "DIGI",
    "MAXIS": "MAXIS",
    "CELCOM": "CELCOM",
    "UM": "UM",
    "DNB": "DNB",
    "YTL": "YTL",
}
BOARD_TYPES = (
    "UNS4MP",
    "UNQ2",
    "U220",
    "U402",
    "EX10",
    "E224",
    "EX2",
    "EM20",
    "HUNS3",
)

DROP_OPERATOR_RULES = {
    "DIGI": "DIGI",
    "CELCOM": "CELCOM",
    "MAXIS": "MAXIS",
    "UM": "UM",
    "UMOBILE": "UM",
    "U MOBILE": "UM",
    "YTL": "YTL",
    "DNB": "DNB",
}
def normalize_capacity_match_name(service_name):
    name = str(service_name).upper().strip()

    # remove operator prefix
    for op in ["DIGI", "MAXIS", "CELCOM", "UMOBILE", "U MOBILE", "UM", "YTL", "DNB"]:
        if name.startswith(op + " "):
            name = name[len(op):].strip()
            break

    # remove site codes like (7107B)
    name = re.sub(r"\([A-Z0-9]+\)?", "", name)

    # remove trailing codes like 7683B / 7641A / 7230R
    name = re.sub(r"\b\d{4,5}[A-Z]\b", "", name)

    # cleanup
    name = name.replace("_", "-")
    name = re.sub(r"\s+", " ", name).strip()

    return name
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

def convert_bps(series):
    s = (
        series.fillna("0")
        .astype(str)
        .str.upper()
        .str.replace(",", "", regex=False)
        .str.strip()
    )

    # Last character determines the unit
    unit = s.str[-1]

    # Remove the unit only when present
    number = s.where(~unit.isin(["K", "M", "G"]), s.str[:-1])

    number = pd.to_numeric(number, errors="coerce").fillna(0)

    multiplier = (
        unit.map({
            "K": 1e3,
            "M": 1e6,
            "G": 1e9,
        })
        .fillna(1)
    )

    return number * multiplier

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
    upper = str(resource_name).upper()

    for board in BOARD_TYPES:
        if board in upper:
            return board

    return "OTHER"

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

def normalize_access_service_name(access_name):
    if access_name is None:
        return ""

    access_name = str(access_name).upper().strip()

    if not access_name or access_name == "NAN":
        return ""

    access_name = access_name.replace("_", "-")

    access_name = re.sub(
        r"\s*\((?:P|W|PROTECTION|PRIMARY|SECONDARY)\)?\s*$",
        "",
        access_name,
        flags=re.IGNORECASE,
    )

    access_name = re.sub(
        r"\s*\((?:P|W|PROTECTION|PRIMARY|SECONDARY)\).*$",
        "",
        access_name,
        flags=re.IGNORECASE,
    )

    access_name = re.sub(
        r"\s*(?:1ST|2ND|3RD)\s*(?:SHARING|GE)?.*$",
        "",
        access_name,
        flags=re.IGNORECASE,
    )

    access_name = re.sub(r"\s+", " ", access_name).strip()
    access_name = re.sub(r"-{2,}", "-", access_name).strip("- ")

    return access_name

@lru_cache(maxsize=50000)
def parse_resource_once(resource_name):

    text = str(resource_name)
    upper = text.upper()

    result = {
        "Ring": "",
        "Endpoint": "",
        "Source Site": "",
        "Sink Site": "",
        "100G Link": "",
        "Board Type": "OTHER",
        "Link Instance": "",
        "Service Group": "",
        "Service Name": "",
        "Access Service Type": "",
        "Access Service Name": "",
    }

    # Ring
    m = RING_RE.search(text)
    if m:
        result["Ring"] = m.group(1)

    # Endpoint
    result["Endpoint"] = text.split("-MAC")[0].strip()

    # 100G Link
    if "[100G LINK] TO" in upper:

        left, right = re.split(r"\[100G LINK\] TO", text, flags=re.IGNORECASE)

        source = left.split("-Shelf")[0].strip(" -")

        sink = right.split("-MAC")[0].strip(" -")

        result["Source Site"] = source
        result["Sink Site"] = sink
        result["100G Link"] = f"{source} -> {sink}"

    else:

        result["Source Site"] = text.split("-Shelf")[0].strip(" -")

    # Board Type
    for board in (
        "UNS4MP",
        "UNQ2",
        "U220",
        "U402",
        "EX10",
        "E224",
        "EX2",
        "EM20",
        "HUNS3",
    ):
        if board in upper:
            result["Board Type"] = board
            break

    # Link Instance
    m = LINK_INSTANCE_RE.search(upper)
    if m:
        result["Link Instance"] = m.group(1)

    # DROP Service
    if "DROP" in upper:
        drop_part = upper

        for keyword, operator in DROP_OPERATOR_RULES.items():
            if keyword in drop_part:
                result["Service Name"] = operator
                break

    # Access Service
    # Access Service
    board_pos = upper.find("K1EX10")

    if board_pos == -1:
        board_pos = upper.find("K1E224")

    if board_pos != -1:
        start = text.find("(", board_pos)

        end = upper.find("-MAC", start)

        if end == -1:
            end = upper.find(":MAC", start)

        if end == -1:
            end = upper.find(": MAC", start)

        if start != -1 and end != -1 and end > start:
            access_name = text[start + 1:end].strip()

            # Remove extra closing bracket if exists
            access_name = access_name.rstrip(")").strip()

            access_name = normalize_access_service_name(access_name)
            if not access_name:
                
                 return result
            result["Access Service Name"] = access_name

            for keyword, service_type in ACCESS_SERVICE_RULES.items():
                if access_name.startswith(keyword):

                    if keyword == "OLT":
                        if "[PRIMARY]" not in access_name and "[SECONDARY]" not in access_name:
                            break

                    result["Access Service Type"] = service_type
                    break

    # Service Group
    if "DIGI" in upper or "CELCOM" in upper:
        result["Service Group"] = "Digi/Celcom"

    elif "MAXIS" in upper:
        result["Service Group"] = "Maxis"

    elif "YTL" in upper or "UMOBILE" in upper or "U MOBILE" in upper or re.search(r"\bUM\b", upper):
        result["Service Group"] = "YTL/UM"

    elif "OLT" in upper or "DNB" in upper:
        result["Service Group"] = "OLT/DNB"

    return result
def prepare_dataframe(combined):
    import time
    raw_time = (
    combined["Collection Time"]
    .astype(str)
    .str.strip()
    .str.strip("'")
    .str.strip('"')
)
    combined = combined.copy()

    start = time.perf_counter()
    combined["Collection Time"] = pd.to_datetime(
        raw_time,
        format="%d/%m/%Y %I:%M:%S %p",
        errors="coerce"
    )   
    print(f"Datetime parse: {time.perf_counter()-start:.2f}s")

    start = time.perf_counter()
    combined["TX_bps"] = convert_bps(combined["TXBPS(bit/s)"])
    combined["RX_bps"] = convert_bps(combined["RXBPS(bit/s)"])
    print(f"Parse TX/RX bps: {time.perf_counter()-start:.2f}s")

    start = time.perf_counter()
    combined["MAX_bps"] = combined[["TX_bps", "RX_bps"]].max(axis=1)
    print(f"MAX bps: {time.perf_counter()-start:.2f}s")

    start = time.perf_counter()
    print("Total rows:", len(combined))
    print("Unique Resource Names:", combined["Resource Name"].nunique())
    parsed_df = pd.DataFrame(
        combined["Resource Name"].apply(parse_resource_once).tolist(),
        index=combined.index
    )
    print(f"Parse Resource Name: {time.perf_counter()-start:.2f}s")

    start = time.perf_counter()
    combined = pd.concat([combined, parsed_df], axis=1)
    print(f"Concat: {time.perf_counter()-start:.2f}s")

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
    if board_set == {"E224", "EM20"}:
        return "E224/EM20"
    if board_set == {"U402", "UNS4MP"}:
        return "U402/UNS4MP"
    if board_set == {"EX2", "EM20"}:
        return "EX2/EM20"
    if board_set == {"EX10", "EX2"}:
        return "EX10/EX2"
    if board_set == {"EX10", "HUNS3"}:
        return "EX10/HUNS3"
    # Otherwise keep a single board type label when possible.
    if len(board_types) == 1:
        return board_types[0]
    # For unexpected mixed groups, keep them visible instead of hiding them.
    return "/".join(board_types)

### new added 23rd may
def normalize_ring_name(value):
    text = str(value).strip().upper()
    text = text.replace("[", "").replace("]", "")
    text = re.sub(r"\.\d+$", "", text)
    return text

##### new added 23rd may
def normalize_board_pair(value):
    parts = [p.strip().upper() for p in str(value).split("/") if p.strip()]
    return "/".join(sorted(parts))

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

def build_service_peak_summary(df):
    service_df = df[df["Service Name"] != ""].dropna(
        subset=["Collection Time", "Resource Name", "TX_bps", "RX_bps"]
    ).copy()

    output_columns = [
        "Service Name",
        "Resource Name",
        "Peak Time",
        "TX (Gbps)",
        "RX (Gbps)",
        "Peak Direction",
    ]

    if service_df.empty:
        return pd.DataFrame(columns=output_columns)

    service_df["TX_bps"] = pd.to_numeric(
        service_df["TX_bps"], errors="coerce"
    ).fillna(0)

    service_df["RX_bps"] = pd.to_numeric(
        service_df["RX_bps"], errors="coerce"
    ).fillna(0)

    service_df["MAX_bps"] = service_df[
        ["TX_bps", "RX_bps"]
    ].max(axis=1)

    # Sort biggest first
    service_df = service_df.sort_values(
        "MAX_bps",
        ascending=False
    )

    # Take highest row for each service+resource
    result = (
        service_df
        .groupby(
            ["Service Name", "Resource Name"],
            as_index=False
        )
        .first()
    )

    result["Peak Time"] = result["Collection Time"]

    result["TX (Gbps)"] = (
        result["TX_bps"]/1e9
    ).round(3)

    result["RX (Gbps)"] = (
        result["RX_bps"]/1e9
    ).round(3)

    result["Peak Direction"] = result.apply(
        lambda x:
        "TX"
        if x["TX_bps"] >= x["RX_bps"]
        else "RX",
        axis=1
    )

    return result[output_columns].sort_values(
        ["Service Name","Resource Name"]
    ).reset_index(drop=True)

def build_ring_peak_summary(df):
    ring_df = df[df["Ring"] != ""].dropna(
        subset=["Collection Time", "TX_bps"]
    ).copy()

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

    # =========================
    # U220 / UNQ2
    # =========================
    link_df = ring_df[
        (ring_df["Board Type"].isin(["UNQ2", "U220"])) &
        (ring_df["Link Instance"] != "") &
        (ring_df["Service Group"].astype(str) != "")
    ].copy()

    if not link_df.empty:

        def u220_monitor_key(row):
            ring = str(row["Ring"])
            instance = str(row["Link Instance"])
            service = str(row["Service Group"])

            same_instance_only = {
                "YTL/UM/OLT/DNB",
                "Digi/Celcom/Maxis",
            }

            if service in same_instance_only:
                return f"{ring}|SERVICE|{service}|INSTANCE|{instance}"

            return f"{ring}|SERVICE|{service}"

        link_df["Monitor Key"] = link_df.apply(u220_monitor_key, axis=1)
        link_df["Board Pair"] = (
            "U220/UNQ2 (" + link_df["Service Group"].astype(str) + ")"
        )

        endpoint_time_totals = (
            link_df.groupby(
                [
                    "Ring",
                    "Monitor Key",
                    "Service Group",
                    "Collection Time",
                    "Endpoint",
                ],
                as_index=False,
            )["TX_bps"]
            .sum()
        )

        endpoint_avg_lookup = (
            endpoint_time_totals.groupby(
                ["Ring", "Monitor Key", "Service Group", "Endpoint"]
            )["TX_bps"]
            .mean()
            .to_dict()
        )

        timestamp_totals = (
            endpoint_time_totals.groupby(
                ["Ring", "Monitor Key", "Service Group", "Collection Time"],
                as_index=False,
            )["TX_bps"]
            .sum()
            .rename(columns={"TX_bps": "Total_TX_bps"})
        )

        peak_idx = timestamp_totals.groupby(
            ["Ring", "Monitor Key", "Service Group"]
        )["Total_TX_bps"].idxmax()

        peak_times = timestamp_totals.loc[peak_idx]

        link_group_lookup = {
            key: grp
            for key, grp in link_df.groupby(
                ["Ring", "Monitor Key", "Service Group"]
            )
        }

        endpoint_group_lookup = {
            key: grp
            for key, grp in endpoint_time_totals.groupby(
                ["Ring", "Monitor Key", "Service Group"]
            )
        }

        for _, peak in peak_times.iterrows():
            ring = peak["Ring"]
            monitor_key = peak["Monitor Key"]
            service_group = peak["Service Group"]
            peak_time = peak["Collection Time"]

            key = (ring, monitor_key, service_group)

            ring_grp = endpoint_group_lookup.get(key)

            if ring_grp is None or ring_grp.empty:
                continue

            same_time_grp = (
                ring_grp[ring_grp["Collection Time"] == peak_time]
                .sort_values("TX_bps", ascending=False)
                .reset_index(drop=True)
            )

            ep1 = same_time_grp.iloc[0]["Endpoint"] if len(same_time_grp) >= 1 else ""
            tx1 = float(same_time_grp.iloc[0]["TX_bps"]) if len(same_time_grp) >= 1 else 0.0

            ep2 = same_time_grp.iloc[1]["Endpoint"] if len(same_time_grp) >= 2 else ""
            tx2 = float(same_time_grp.iloc[1]["TX_bps"]) if len(same_time_grp) >= 2 else 0.0

            total = tx1 + tx2

            avg_ep1_bps = float(
                endpoint_avg_lookup.get(
                    (ring, monitor_key, service_group, ep1),
                    0.0,
                )
            )

            avg_ep2_bps = float(
                endpoint_avg_lookup.get(
                    (ring, monitor_key, service_group, ep2),
                    0.0,
                )
            )

            avg_total_bps = avg_ep1_bps + avg_ep2_bps
            peak_avg_ratio = total / avg_total_bps if avg_total_bps > 0 else 0.0

            raw_group = link_group_lookup.get(key)

            if raw_group is None or raw_group.empty:
                display_instance = ""
                max_capacity = 0
            else:
                instance_list = sorted(
                    {
                        str(v)
                        for v in raw_group["Link Instance"].dropna().astype(str)
                        if str(v).strip()
                    }
                )
                display_instance = "/".join(instance_list)
                max_capacity = calculate_group_capacity(raw_group)

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
                "Max Capacity (Gbps)": max_capacity,
            })

    # =========================
    # Non U220 / UNQ2
    # =========================
    non_link_df = ring_df[
        ~ring_df["Board Type"].isin(["UNQ2", "U220"])
    ].copy()

    if not non_link_df.empty:

        def assign_non_link_group(row, ring_type_map):
            board_type = str(row["Board Type"])
            ring = row["Ring"]
            ring_types = ring_type_map.get(ring, set())

            if board_type in {"E224", "EX10"} and ring_types.issuperset({"E224", "EX10"}):
                return "E224/EX10"

            if board_type in {"U402", "UNS4MP"} and ring_types.issuperset({"U402", "UNS4MP"}):
                return "U402/UNS4MP"

            if board_type in {"E224", "EM20"} and ring_types.issuperset({"E224", "EM20"}):
                return "E224/EM20"

            if board_type in {"EX2", "EM20"} and ring_types.issuperset({"EX2", "EM20"}):
                return "EX2/EM20"

            if board_type in {"EX2", "EX10"} and ring_types.issuperset({"EX2", "EX10"}):
                return "EX10/EX2"

            if board_type in {"HUNS3", "EX10"} and ring_types.issuperset({"HUNS3", "EX10"}):
                return "EX10/HUNS3"

            return board_type

        ring_type_map = (
            non_link_df.groupby("Ring")["Board Type"]
            .apply(lambda s: {str(v) for v in s.dropna().astype(str) if v and v != "OTHER"})
            .to_dict()
        )

        non_link_df["Board Pair"] = non_link_df.apply(
            lambda row: assign_non_link_group(row, ring_type_map),
            axis=1,
        )

        endpoint_time_totals = (
            non_link_df.groupby(
                ["Ring", "Board Pair", "Collection Time", "Endpoint"],
                as_index=False,
            )["TX_bps"]
            .sum()
        )

        endpoint_avg_lookup = (
            endpoint_time_totals.groupby(
                ["Ring", "Board Pair", "Endpoint"]
            )["TX_bps"]
            .mean()
            .to_dict()
        )

        timestamp_totals = (
            endpoint_time_totals.groupby(
                ["Ring", "Board Pair", "Collection Time"],
                as_index=False,
            )["TX_bps"]
            .sum()
            .rename(columns={"TX_bps": "Total_TX_bps"})
        )

        peak_idx = timestamp_totals.groupby(
            ["Ring", "Board Pair"]
        )["Total_TX_bps"].idxmax()

        peak_times = timestamp_totals.loc[peak_idx]

        endpoint_group_lookup = {
            key: grp
            for key, grp in endpoint_time_totals.groupby(["Ring", "Board Pair"])
        }

        raw_group_lookup = {
            key: grp
            for key, grp in non_link_df.groupby(["Ring", "Board Pair"])
        }

        for _, peak in peak_times.iterrows():
            ring = peak["Ring"]
            board_pair = peak["Board Pair"]
            peak_time = peak["Collection Time"]

            key = (ring, board_pair)

            ring_grp = endpoint_group_lookup.get(key)

            if ring_grp is None or ring_grp.empty:
                continue

            same_time_grp = (
                ring_grp[ring_grp["Collection Time"] == peak_time]
                .sort_values("TX_bps", ascending=False)
                .reset_index(drop=True)
            )

            ep1 = same_time_grp.iloc[0]["Endpoint"] if len(same_time_grp) >= 1 else ""
            tx1 = float(same_time_grp.iloc[0]["TX_bps"]) if len(same_time_grp) >= 1 else 0.0

            ep2 = same_time_grp.iloc[1]["Endpoint"] if len(same_time_grp) >= 2 else ""
            tx2 = float(same_time_grp.iloc[1]["TX_bps"]) if len(same_time_grp) >= 2 else 0.0

            total = tx1 + tx2

            avg_ep1_bps = float(
                endpoint_avg_lookup.get(
                    (ring, board_pair, ep1),
                    0.0,
                )
            )

            avg_ep2_bps = float(
                endpoint_avg_lookup.get(
                    (ring, board_pair, ep2),
                    0.0,
                )
            )

            avg_total_bps = avg_ep1_bps + avg_ep2_bps
            peak_avg_ratio = total / avg_total_bps if avg_total_bps > 0 else 0.0

            raw_group = raw_group_lookup.get(key)
            max_capacity = calculate_group_capacity(raw_group) if raw_group is not None else 0

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
                "Max Capacity (Gbps)": max_capacity,
            })

    if not result_rows:
        return pd.DataFrame(columns=output_columns)

    peaks = pd.DataFrame(result_rows)

    peaks["Util %"] = (
        peaks["Total TX (Gbps)"] /
        peaks["Max Capacity (Gbps)"] *
        100
    ).round(1)

    peaks["Util %"] = peaks["Util %"].replace([float("inf"), -float("inf")], 0).fillna(0)

    peaks["Util Band"] = peaks["Total TX (Gbps)"].apply(util_band_ring)

    return peaks[output_columns].sort_values(
        ["Total TX (Gbps)", "Ring", "Board Pair"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

def build_100g_peak_summary(df):
    g100_df = df[df["100G Link"] != ""].dropna(
        subset=["Collection Time", "MAX_bps"]
    ).copy()

    output_columns = [
        "100G Link",
        "Source Site",
        "Sink Site",
        "Peak Time",
        "Peak Util (Gbps)",
        "Average Util (Gbps)",
        "BH Avg 20-22 (Gbps)",
        "Peak Average Ratio",
        "Util Band",
    ]

    if g100_df.empty:
        return pd.DataFrame(columns=output_columns)

    grouped = g100_df.groupby(
        ["100G Link", "Source Site", "Sink Site", "Collection Time"],
        as_index=False
    )["MAX_bps"].sum()

    result_rows = []

    for (link_name, source_site, sink_site), grp in grouped.groupby(
        ["100G Link", "Source Site", "Sink Site"]
    ):
        if grp.empty:
            continue

        grp = grp.copy()
        grp["Collection Time"] = pd.to_datetime(
            grp["Collection Time"],
            errors="coerce"
        )

        grp = grp.dropna(subset=["Collection Time"])

        if grp.empty:
            continue

        grp = grp.sort_values("MAX_bps", ascending=False).reset_index(drop=True)
        peak_row = grp.iloc[0]

        max_bps_series = pd.to_numeric(grp["MAX_bps"], errors="coerce")

        peak_bps = float(max_bps_series.iloc[0]) if not max_bps_series.empty else 0.0
        avg_bps = float(max_bps_series.mean()) if not max_bps_series.empty else 0.0
        par = (peak_bps / avg_bps) if avg_bps > 0 else 0.0

        bh_grp = grp[
            (grp["Collection Time"].dt.hour >= 20) &
            (grp["Collection Time"].dt.hour <22)
            |
            (grp["Collection Time"].dt.hour == 22) &
            (grp["Collection Time"].dt.minute == 0)
        ].copy()

        if not bh_grp.empty:
            bh_avg_bps = float(
                pd.to_numeric(
                    bh_grp["MAX_bps"],
                    errors="coerce"
                ).mean()
            )
        else:
            bh_avg_bps = 0.0

        result_rows.append({
            "100G Link": link_name,
            "Source Site": source_site,
            "Sink Site": sink_site,
            "Peak Time": peak_row["Collection Time"],
            "Peak Util (Gbps)": round(peak_bps / 1e9, 3),
            "Average Util (Gbps)": round(avg_bps / 1e9, 3),
            "BH Avg 20-22 (Gbps)": round(bh_avg_bps / 1e9, 3),
            "Peak Average Ratio": round(par, 2),
            "Util Band": util_band_100g(peak_bps / 1e9),
        })

    if not result_rows:
        return pd.DataFrame(columns=output_columns)

    peaks = pd.DataFrame(result_rows)

    return peaks[output_columns].sort_values(
        ["Peak Util (Gbps)", "100G Link"],
        ascending=[False, True]
    ).reset_index(drop=True)
    
def build_ring_proof(df, ring_name, board_pair="", link_instance=""):
    ring_df = df[df["Ring"].astype(str) == str(ring_name)].dropna(
        subset=["Collection Time", "TX_bps"]
    ).copy()

    if ring_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 1) Filter board pair first
    if board_pair:
        board_pair_text = str(board_pair)

        if board_pair_text == "E224/EX10":
            ring_df = ring_df[ring_df["Board Type"].astype(str).isin(["E224", "EX10"])].copy()

        elif board_pair_text == "U402/UNS4MP":
            ring_df = ring_df[ring_df["Board Type"].astype(str).isin(["U402", "UNS4MP"])].copy()

        elif board_pair_text == "EX2/EX10":
            ring_df = ring_df[
                ring_df["Board Type"].astype(str).isin(["EX2", "EX10"])
            ].copy()

        elif board_pair_text == "EX2/EM20":
            ring_df = ring_df[ring_df["Board Type"].astype(str).isin(["EX2", "EM20"])].copy()

        elif board_pair_text.startswith("U220/UNQ2 (") and board_pair_text.endswith(")"):
            fallback_group = board_pair_text[len("U220/UNQ2 ("):-1]

            ring_df = ring_df[
                ring_df["Board Type"].astype(str).isin(["U220", "UNQ2"]) &
                (ring_df["Service Group"].astype(str) == fallback_group)
            ].copy()

        elif board_pair_text in ["U220/UNQ2", "UNQ2/U220"]:
            ring_df = ring_df[
                ring_df["Board Type"].astype(str).isin(["U220", "UNQ2"])
            ].copy()

        elif board_pair_text == "UNQ2/UNQ2":
            ring_df = ring_df[
                ring_df["Board Type"].astype(str) == "UNQ2"
            ].copy()

        else:
            ring_df = ring_df[
                ring_df["Board Type"].astype(str) == board_pair_text
            ].copy()

    # 2) Then filter link instance
    if link_instance:
        ring_df = ring_df[
            ring_df["Link Instance"].astype(str) == str(link_instance)
        ].copy()

    if ring_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    endpoint_totals = ring_df.groupby(
        ["Collection Time", "Endpoint"],
        as_index=False
    )["TX_bps"].sum()

    endpoint_totals["TX (Gbps)"] = (endpoint_totals["TX_bps"] / 1e9).round(3)

    timestamp_totals = endpoint_totals.groupby(
        "Collection Time",
        as_index=False
    )["TX_bps"].sum().rename(columns={"TX_bps": "Total_TX_bps"})

    timestamp_totals["Total TX (Gbps)"] = (
        timestamp_totals["Total_TX_bps"] / 1e9
    ).round(3)

    peak_time = timestamp_totals.loc[
        timestamp_totals["Total_TX_bps"].idxmax(),
        "Collection Time"
    ]

    same_time = endpoint_totals[
        endpoint_totals["Collection Time"] == peak_time
    ].sort_values("TX_bps", ascending=False).reset_index(drop=True)

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

def build_ftth_peak_summary(df):
    ftth_df = df.copy()
    ftth_df["OLT Name"] = ftth_df["Resource Name"].apply(extract_olt_name)

    ftth_df = ftth_df[ftth_df["OLT Name"] != ""].dropna(
        subset=["Collection Time", "TX_bps","RX_bps"]
    ).copy()

    output_columns = [
        "OLT Name",
        "Resource Name",
        "Peak Time",
        "TX (Gbps)",
        "RX (Gbps)",
        "Peak Direction",
        "Peak Util (Gbps)",
    ]
    
    if ftth_df.empty:
        return pd.DataFrame(columns=output_columns)
    
    ftth_df["TX_bps"] = pd.to_numeric(ftth_df["TX_bps"], errors="coerce").fillna(0)
    ftth_df["RX_bps"] = pd.to_numeric(ftth_df["RX_bps"], errors="coerce").fillna(0)

    # Select highest between TX and RX for each row
    ftth_df["MAX_bps"] = ftth_df[["TX_bps", "RX_bps"]].max(axis=1)

    # Sort highest first
    ftth_df = ftth_df.sort_values("MAX_bps", ascending=False)

    # For each OLT, take the row with highest TX/RX
    result = ftth_df.groupby("OLT Name", as_index=False).first()

    result["Peak Time"] = result["Collection Time"]
    result["TX (Gbps)"] = (result["TX_bps"] / 1e9).round(3)
    result["RX (Gbps)"] = (result["RX_bps"] / 1e9).round(3)
    result["Peak Util (Gbps)"] = (result["MAX_bps"] / 1e9).round(3)

    result["Peak Direction"] = result.apply(
        lambda r: "TX" if r["TX_bps"] >= r["RX_bps"] else "RX",
        axis=1
    )

    return result[output_columns].sort_values(
        ["Peak Util (Gbps)", "OLT Name"],
        ascending=[False, True]
    ).reset_index(drop=True)

def build_access_service_summary(df):
    access_df = df[
        (df["Access Service Type"] != "") &
        (df["Access Service Name"] != "")
    ].copy()

    output_columns = [
        "Service Type",
        "Service Name",
        "Resource Name",
        "Peak Time",
        "TX (Gbps)",
        "RX (Gbps)",
        "Peak Direction",
        "Peak Util (Gbps)",
        "Average Util (Gbps)",
        "Current Capacity (Gbps)",
        "Util %",
    ]

    if access_df.empty:
        return pd.DataFrame(columns=output_columns)

    access_df["TX_bps"] = pd.to_numeric(access_df["TX_bps"], errors="coerce").fillna(0)
    access_df["RX_bps"] = pd.to_numeric(access_df["RX_bps"], errors="coerce").fillna(0)
    access_df["MAX_bps"] = access_df[["TX_bps", "RX_bps"]].max(axis=1)

    access_df = access_df.sort_values("MAX_bps", ascending=False)

    result = access_df.groupby(
        ["Access Service Type", "Access Service Name"],
        as_index=False
    ).first()

    avg_df = access_df.groupby(
        ["Access Service Type", "Access Service Name"],
        as_index=False
    )["MAX_bps"].mean().rename(columns={"MAX_bps": "AVG_bps"})

    result = result.merge(
        avg_df,
        on=["Access Service Type", "Access Service Name"],
        how="left"
    )

    result["Service Type"] = result["Access Service Type"]
    result["Service Name"] = result["Access Service Name"]
    result["Peak Time"] = result["Collection Time"]
    result["TX (Gbps)"] = (result["TX_bps"] / 1e9).round(3)
    result["RX (Gbps)"] = (result["RX_bps"] / 1e9).round(3)
    result["Peak Util (Gbps)"] = (result["MAX_bps"] / 1e9).round(3)
    result["Average Util (Gbps)"] = (result["AVG_bps"] / 1e9).round(3)
    capacity_lookup = {
    normalize_capacity_match_name(x.site_name): x.current_capacity
    for x in AccessBandwidth.objects.all()
}
    match_names = result["Service Name"].apply(normalize_capacity_match_name)

    result["Current Capacity (Gbps)"] = (
        match_names.map(capacity_lookup).fillna(0) / 1000
    ).round(3)

    result["Util %"] = (
        result["Peak Util (Gbps)"] /
        result["Current Capacity (Gbps)"] *
        100
    ).round(1)

    result.loc[result["Current Capacity (Gbps)"] == 0, "Util %"] = 0
    result["Peak Direction"] = result.apply(
        lambda r: "TX" if r["TX_bps"] >= r["RX_bps"] else "RX",
        axis=1
    )

    return result[output_columns].sort_values(
        ["Service Type", "Peak Util (Gbps)", "Service Name"],
        ascending=[True, False, True]
    ).reset_index(drop=True)

def to_excel_bytes(ring_peaks, g100_peaks, service_peaks=None, ftth_peaks=None,ring_node_details=None):
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="dd/mm/yyyy hh:mm") as writer:
        ring_peaks.to_excel(writer, sheet_name="Ring_Peak_Summary", index=False)
        g100_peaks.to_excel(writer, sheet_name="100G_Peak_Summary", index=False)

        if service_peaks is not None:
            service_peaks.to_excel(writer, sheet_name="Service_Peak_Summary", index=False)

        if ftth_peaks is not None:
            ftth_peaks.to_excel(writer, sheet_name="FTTH_Peak_Summary", index=False)

        valid_node_detail = (
            ring_node_details is not None
            and not ring_node_details.empty
            and all(c in ring_node_details.columns for c in ["Ring", "Board Pair", "Source NE", "Sink NE"])
            and "Ring" in ring_peaks.columns
            and "Board Pair" in ring_peaks.columns
        )

        if valid_node_detail:
            ring_node_details.to_excel(writer, sheet_name="Ring_Node_Detail", index=False)

            ring_ws = writer.sheets["Ring_Peak_Summary"]
            node_sheet_name = "Ring_Node_Detail"
            ring_col = ring_peaks.columns.get_loc("Ring")

            for row_idx, row in ring_peaks.iterrows():
                ring_value = str(row.get("Ring", ""))
                board_pair = str(row.get("Board Pair", ""))

                match_df = ring_node_details[
                    (ring_node_details["Ring"].astype(str) == ring_value) &
                    (ring_node_details["Board Pair"].astype(str) == board_pair)
                ]

                if not match_df.empty:
                    excel_row = row_idx + 1
                    target_row = match_df.index[0] + 2

                    ring_ws.write_url(
                        excel_row,
                        ring_col,
                        f"internal:'{node_sheet_name}'!A{target_row}",
                        string=ring_value
                    )

    output.seek(0)
    return output.getvalue()
