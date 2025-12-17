import io
import os
import re
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd

from report_schema import get_s3_client, parse_s3_uri, read_bytes_from_s3


REPORT_TYPE_ROMAN = {
    1: "I",
    2: "II",
    3: "III",
    4: "IV",
    5: "V",
}


def parse_report_type_key(report_type: Union[int, str]) -> Tuple[int, Optional[str]]:
    if isinstance(report_type, int):
        return report_type, None

    s = str(report_type).strip()
    if not s:
        raise RuntimeError("Empty report_type")

    if "_" in s:
        base_str, subtype = s.split("_", 1)
        base = int(base_str)
        subtype = subtype.strip() or None
        return base, subtype

    return int(s), None


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
            "Missing S3 input path. "
            "Set env vars S3_BUCKET_NAME + S3_INPUT_PREFIX "
            "or define sources[...].path in config."
        )

    return path


def _find_latest_csv_for_report_type(
    prefix_uri: str,
    report_type: Union[int, str],
) -> Tuple[str, str]:
    base_type, subtype = parse_report_type_key(report_type)

    roman = REPORT_TYPE_ROMAN.get(base_type)
    if not roman:
        raise RuntimeError(f"Unsupported report_type: {report_type}")

    bucket, prefix = parse_s3_uri(prefix_uri)
    prefix = prefix.rstrip("/") + "/"

    if subtype:
        file_prefix = f"{prefix}FT_BC_OC_REPORT_{roman}_{subtype}_"
        pattern = re.compile(
            rf"FT_BC_OC_REPORT_{roman}_{re.escape(subtype)}_(\d{{8}})\.csv$",
            re.IGNORECASE,
        )
    else:
        file_prefix = f"{prefix}FT_BC_OC_REPORT_{roman}_"
        pattern = re.compile(
            rf"FT_BC_OC_REPORT_{roman}_(\d{{8}})\.csv$",
            re.IGNORECASE,
        )

    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")

    best_dt: Optional[datetime] = None
    best_key: Optional[str] = None

    for page in paginator.paginate(Bucket=bucket, Prefix=file_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key.rsplit("/", 1)[-1]

            m = pattern.match(name)
            if not m:
                continue

            try:
                dt = datetime.strptime(m.group(1), "%Y%m%d")
            except ValueError:
                continue

            if (
                best_dt is None
                or dt > best_dt
                or (dt == best_dt and (best_key is None or key > best_key))
            ):
                best_dt = dt
                best_key = key

    if not best_key or not best_dt:
        raise RuntimeError(
            f"No matching CSV found for report_type={report_type} under {prefix_uri}"
        )

    return f"s3://{bucket}/{best_key}", best_dt.strftime("%Y%m%d")


def load_source_dataframe_for_report_type(
    config: Dict[str, Any],
    source_name: str,
    report_type: Union[int, str],
) -> Tuple[pd.DataFrame, str]:
    sources = config.get("sources", {})
    src_conf = sources.get(source_name, {})

    prefix_uri = _get_input_prefix_uri(config, source_name)
    delimiter = src_conf.get("delimiter", ",")
    encoding = src_conf.get("encoding", "utf-8")
    header_flag = src_conf.get("header", True)

    latest_uri, input_yyyymmdd = _find_latest_csv_for_report_type(prefix_uri, report_type)
    data_bytes = read_bytes_from_s3(latest_uri)

    df = pd.read_csv(
        io.BytesIO(data_bytes),
        sep=delimiter,
        encoding=encoding,
        header=0 if header_flag else None,
        dtype=str,
        keep_default_na=False,
    )

    return df, input_yyyymmdd