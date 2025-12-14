import io
import pandas as pd
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

from clients_rules import filter_dataframe_for_report_type
from report_schema import get_s3_client, get_logical_fields_for_type, get_numeric_logical_fields, get_source_columns_map, load_config, parse_s3_uri, read_bytes_from_s3

REPORT_TYPE_ROMAN = {
    1: "I",
    2: "II",
    3: "III",
    4: "IV",
    5: "V",
}


def _get_latest_csv_uri_for_report(prefix_uri: str, report_type: int) -> Tuple[str, str]:
    roman = REPORT_TYPE_ROMAN.get(report_type)
    if not roman:
        raise RuntimeError(f"Unsupported report_type: {report_type}")

    bucket, prefix = parse_s3_uri(prefix_uri)
    prefix = prefix.rstrip("/") + "/"

    file_prefix = f"{prefix}FT_BC_OC_REPORT_{roman}_"
    pattern = re.compile(rf"FT_BC_OC_REPORT_{roman}_(\d{{8}})\.csv$", re.IGNORECASE)

    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")

    best_date = None
    best_key = None

    for page in paginator.paginate(Bucket=bucket, Prefix=file_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            basename = key.rsplit("/", 1)[-1]
            m = pattern.match(basename)
            if not m:
                continue
            date_str = m.group(1)

            try:
                dt = datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                continue

            if best_date is None or dt > best_date or (dt == best_date and (best_key is None or key > best_key)):
                best_date = dt
                best_key = key

    if not best_key or not best_date:
        raise RuntimeError(f"No matching CSV found for report_type={report_type} under {prefix_uri}")

    return f"s3://{bucket}/{best_key}", best_date.strftime("%Y%m%d")


def _load_source_dataframe_for_report(config: Dict[str, Any], source_name: str, report_type: int) -> Tuple[pd.DataFrame, str]:
    sources = config["sources"]
    if source_name not in sources:
        raise KeyError(f"Source '{source_name}' not defined in config['sources']")

    src_conf = sources[source_name]
    prefix_uri = src_conf["path"]
    delimiter = src_conf.get("delimiter", ",")
    encoding = src_conf.get("encoding", "utf-8")
    header_flag = src_conf.get("header", True)
    decimal_sep = src_conf.get("decimal", ".")
    thousands_sep = src_conf.get("thousands", None)

    latest_uri, latest_yyyymmdd = _get_latest_csv_uri_for_report(prefix_uri, report_type)
    data_bytes = read_bytes_from_s3(latest_uri)

    df = pd.read_csv(
        io.BytesIO(data_bytes),
        sep=delimiter,
        encoding=encoding,
        header=0 if header_flag else None,
        decimal=decimal_sep,
        thousands=thousands_sep,
    )
    return df, latest_yyyymmdd


def build_report_excel(report_type: int) -> bytes:
    xlsx_bytes, _, _ = build_report_excel_with_metadata(report_type)
    return xlsx_bytes


def build_report_excel_with_metadata(report_type: int) -> Tuple[bytes, str, str]:
    config: Dict[str, Any] = load_config()

    logical_fields: List[str] = get_logical_fields_for_type(config, report_type)
    source_columns_map: Dict[str, str] = get_source_columns_map(config, logical_fields)

    report_types = config["report_types"]
    source_name = report_types[str(report_type)].get("source", "main_csv")

    df, input_yyyymmdd = _load_source_dataframe_for_report(config, source_name, report_type)

    source_columns = [source_columns_map[lf] for lf in logical_fields]
    missing = [col for col in source_columns if col not in df.columns]
    if missing:
        raise RuntimeError(f"Source CSV is missing columns: {', '.join(missing)}")

    df = df[source_columns].copy()
    rename_map = {source_columns_map[lf]: lf for lf in logical_fields}
    df.rename(columns=rename_map, inplace=True)

    numeric_fields = get_numeric_logical_fields(config, logical_fields)
    for nf in numeric_fields:
        df[nf] = pd.to_numeric(df[nf], errors="coerce")

    df = filter_dataframe_for_report_type(df, report_type)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=f"tipo_{report_type}")
    output.seek(0)

    roman = REPORT_TYPE_ROMAN[report_type]
    return output.read(), input_yyyymmdd, roman
