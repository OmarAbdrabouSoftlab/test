import json
import os
import re

import boto3
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

_S3_CLIENT: Optional[Any] = None



def get_s3_client():
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT


def _as_s3_prefix_uri(bucket: str, key_prefix: str) -> str:
    prefix = key_prefix.strip("/")
    return f"s3://{bucket}/{prefix}/" if prefix else f"s3://{bucket}/"


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    without_scheme = uri[5:]
    parts = without_scheme.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid S3 URI (missing key): {uri}")
    bucket, key = parts
    return bucket, key


def read_text_from_s3(uri: str, encoding: str = "utf-8") -> str:
    client = get_s3_client()
    bucket, key = parse_s3_uri(uri)
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode(encoding)


def read_bytes_from_s3(uri: str) -> bytes:
    client = get_s3_client()
    bucket, key = parse_s3_uri(uri)
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def write_bytes_to_s3(uri: str, data: bytes, content_type: Optional[str] = None) -> None:
    client = get_s3_client()
    bucket, key = parse_s3_uri(uri)
    extra_args: Dict[str, Any] = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if extra_args:
        client.put_object(Bucket=bucket, Key=key, Body=data, **extra_args)
    else:
        client.put_object(Bucket=bucket, Key=key, Body=data)


def delete_s3_prefix(prefix_uri: str) -> None:
    client = get_s3_client()
    bucket, prefix = parse_s3_uri(prefix_uri)
    paginator = client.get_paginator("list_objects_v2")

    batch: List[Dict[str, str]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:
                client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                batch = []

    if batch:
        client.delete_objects(Bucket=bucket, Delete={"Objects": batch})


def get_latest_json_uri(prefix_uri: str) -> str:
    client = get_s3_client()
    bucket, prefix = parse_s3_uri(prefix_uri)
    paginator = client.get_paginator("list_objects_v2")

    latest_key = None
    latest_last_modified = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".json"):
                continue
            lm = obj.get("LastModified")
            if latest_last_modified is None or (lm is not None and lm > latest_last_modified) or (
                lm == latest_last_modified and (latest_key is None or key > latest_key)
            ):
                latest_last_modified = lm
                latest_key = key

    if not latest_key:
        raise RuntimeError(f"No JSON found under {prefix_uri}")

    return f"s3://{bucket}/{latest_key}"


def get_latest_csv_uri(prefix_uri: str) -> str:
    client = get_s3_client()
    bucket, prefix = parse_s3_uri(prefix_uri)
    paginator = client.get_paginator("list_objects_v2")

    latest_key = None
    latest_blob_key = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".csv"):
                continue
            basename = key.rsplit("/", 1)[-1]
            m = re.search(r"(\d{8})", basename)
            if not m:
                continue
            date_str = m.group(1)
            if latest_key is None or date_str > latest_key or (
                date_str == latest_key and (latest_blob_key is None or key > latest_blob_key)
            ):
                latest_key = date_str
                latest_blob_key = key

    if latest_blob_key is None:
        raise RuntimeError(f"No CSV with yyyymmdd in name found under {prefix_uri}")

    return f"s3://{bucket}/{latest_blob_key}"


def load_config() -> Dict[str, Any]:

    bucket = os.environ.get("S3_BUCKET_NAME")
    prefix = os.environ.get("S3_CONFIG_PREFIX", "Config/")
    if not bucket:
        raise RuntimeError("Missing S3_BUCKET_NAME environment variable")

    prefix_uri = _as_s3_prefix_uri(bucket, prefix)
    config_uri = get_latest_json_uri(prefix_uri)

    raw_json = read_text_from_s3(config_uri)
    raw_stripped = raw_json.strip()
    if not raw_stripped:
        raise RuntimeError(f"Config file is empty or unreadable: {config_uri}")

    return json.loads(raw_stripped)


def get_logical_fields_for_type(config: Dict[str, Any], report_type: int) -> List[str]:
    shared = config["shared_fields"]
    report_types = config["report_types"]
    type_key = str(report_type)
    if type_key not in report_types:
        raise KeyError(f"Unknown report_type: {report_type}")

    specific = report_types[type_key]["specific_fields"]

    result: List[str] = []
    seen = set()

    for field in shared + specific:
        if field not in seen:
            seen.add(field)
            result.append(field)

    return result


def get_source_columns_map(config: Dict[str, Any], logical_fields: List[str]) -> Dict[str, str]:
    fields_def = config["fields"]
    result: Dict[str, str] = {}

    for lf in logical_fields:
        if lf not in fields_def:
            raise KeyError(f"Field '{lf}' not defined in config['fields']")
        result[lf] = fields_def[lf]["source_column"]

    return result


def get_numeric_logical_fields(config: Dict[str, Any], logical_fields: List[str]) -> List[str]:
    fields_def = config["fields"]
    numeric_fields: List[str] = []

    for lf in logical_fields:
        field_conf = fields_def.get(lf, {})
        if field_conf.get("type") == "number":
            numeric_fields.append(lf)

    return numeric_fields


def output_today_prefix_uri(bucket: str, output_prefix: str, today: str) -> str:
    prefix = (output_prefix or "").strip("/")
    if prefix:
        return f"s3://{bucket}/{prefix}/{today}/"
    return f"s3://{bucket}/{today}/"


def parse_report_type_key(report_type_key: str) -> Tuple[int, Optional[str], str]:
    k = (report_type_key or "").strip()
    if "_" in k:
        base_str, subtype = k.split("_", 1)
        return int(base_str), (subtype.strip() or None), k
    return int(k), None, k


def output_report_type_prefix_uri(output_today_uri: str, report_type_key: str) -> str:
    base, subtype, full_key = parse_report_type_key(report_type_key)
    folder = full_key if subtype else str(base)
    base_uri = output_today_uri.rstrip("/") + "/"
    return f"{base_uri}Report_Type_{folder}/"


def sanitize_for_filename(value: Any) -> str:
    s = "" if value is None else str(value)
    s = s.strip()
    if not s:
        return "UNKNOWN"
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^A-Za-z0-9 _.-]", "", s)
    s = s.replace(" ", "_")
    s = s[:120].strip("_")
    return s or "UNKNOWN"


def report_type_keys_for_tipo_report(tipo_report: str, config_report_type_keys: List[str]) -> List[str]:
    t = (tipo_report or "").strip()
    bases: List[int]
    if t == "1":
        bases = [1]
    elif t == "2/3":
        bases = [2, 3]
    elif t == "4/5":
        bases = [4, 5]
    else:
        return []

    out: List[str] = []
    for k in config_report_type_keys:
        b, _, _ = parse_report_type_key(k)
        if b in bases:
            out.append(k)

    out.sort(key=lambda x: (int(x.split("_", 1)[0]), x))
    return out


def match_produced_files_for_client(
    *,
    produced_files: Dict[Tuple[str, str], bytes],
    report_type_key: str,
    client_suffix: str,
) -> List[Tuple[str, bytes]]:
    matched: List[Tuple[str, bytes]] = []
    for (k, suffix), data in produced_files.items():
        if k != report_type_key:
            continue
        if not suffix:
            continue
        if suffix == client_suffix or client_suffix in suffix:
            matched.append((suffix, data))
    return matched