import io
import os
import re
import pandas as pd

from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from report_schema import get_s3_client, parse_s3_uri, read_bytes_from_s3


REPORT_TYPE_ROMAN = {
    1: "I",
    2: "II",
    3: "III",
    4: "IV",
    5: "V",
}


def _get_input_prefix_uri(config: Dict[str, Any], source_name: str) -> str:
    bucket = os.environ.get("S3_BUCKET_NAME")
    input_prefix = os.environ.get("S3_INPUT_PREFIX")

    if bucket and input_prefix:
        prefix = input_prefix.strip("/")
        return f"s3://{bucket}/{prefix}/" if prefix else f"s3://{bucket}/"

    sources = config.get("sources", {})
    if source_name not in sources:
        raise KeyError(f"Source '{source_name}' not defined in config['sources']")

    path = sources[source_name].get("path")
    if not path:
        raise RuntimeError(
            "Missing S3 input path. Set env vars S3_BUCKET_NAME + S3_INPUT_PREFIX "
            "or define sources[...].path in config."
        )
    return path


def _find_latest_csv_for_report_type(prefix_uri: str, report_type: int) -> Tuple[str, str]:
    roman = REPORT_TYPE_ROMAN.get(report_type)
    if not roman:
        raise RuntimeError(f"Unsupported report_type: {report_type}")

    bucket, prefix = parse_s3_uri(prefix_uri)
    prefix = prefix.rstrip("/") + "/"

    # We list only keys under: <prefix>/FT_BC_OC_REPORT_<ROMAN>_
    file_prefix = f"{prefix}FT_BC_OC_REPORT_{roman}_"
    pattern = re.compile(rf"FT_BC_OC_REPORT_{roman}_(\d{{8}})\.csv$", re.IGNORECASE)

    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")

    best_dt: Optional[datetime] = None
    best_key: Optional[str] = None

    for page in paginator.paginate(Bucket=bucket, Prefix=file_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            basename = key.rsplit("/", 1)[-1]

            m = pattern.match(basename)
            if not m:
                continue

            try:
                dt = datetime.strptime(m.group(1), "%Y%m%d")
            except ValueError:
                continue

            if best_dt is None or dt > best_dt or (dt == best_dt and (best_key is None or key > best_key)):
                best_dt = dt
                best_key = key

    if not best_key or not best_dt:
        raise RuntimeError(f"No matching CSV found for report_type={report_type} under {prefix_uri}")

    return f"s3://{bucket}/{best_key}", best_dt.strftime("%Y%m%d")


def _dtype_map_from_schema(config: Dict[str, Any]) -> Dict[str, str]:
    """
    Build dtype overrides from report_schema.json.
    Any field declared as type='string' will be read as pandas 'string' to preserve exact digits.
    """
    dtype_map: Dict[str, str] = {}
    for field_conf in config.get("fields", {}).values():
        if field_conf.get("type") == "string":
            col = field_conf.get("source_column")
            if col:
                dtype_map[col] = "string"
    return dtype_map


def load_source_dataframe_for_report_type(
    config: Dict[str, Any],
    source_name: str,
    report_type: int,
) -> Tuple[pd.DataFrame, str]:
    src_conf = config.get("sources", {}).get(source_name, {})

    prefix_uri = _get_input_prefix_uri(config, source_name)
    delimiter = src_conf.get("delimiter", ",")
    encoding = src_conf.get("encoding", "utf-8")
    header_flag = src_conf.get("header", True)

    latest_uri, latest_yyyymmdd = _find_latest_csv_for_report_type(prefix_uri, report_type)
    data_bytes = read_bytes_from_s3(latest_uri)

    dtype_map = _dtype_map_from_schema(config)

    read_csv_kwargs: Dict[str, Any] = {
        "sep": delimiter,
        "encoding": encoding,
        "header": 0 if header_flag else None,
    }

    if dtype_map:
        read_csv_kwargs["dtype"] = dtype_map
    else:
        # Only relevant when pandas is allowed to parse numerics.
        read_csv_kwargs["decimal"] = src_conf.get("decimal", ".")
        read_csv_kwargs["thousands"] = src_conf.get("thousands", None)

    df = pd.read_csv(io.BytesIO(data_bytes), **read_csv_kwargs)

    print("CSV COLUMNS:", list(df.columns))
    return df, latest_yyyymmdd