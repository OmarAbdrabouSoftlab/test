import boto3
import json
import os
import re

from typing import Any, Dict, List, Optional, Tuple

_S3_CLIENT: Optional[Any] = None
_CONFIG: Optional[Dict[str, Any]] = None


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


def write_bytes_to_s3(uri: str, data: bytes, content_type: str | None = None) -> None:
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
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue
        delete_req = {"Objects": [{"Key": obj["Key"]} for obj in contents]}
        client.delete_objects(Bucket=bucket, Delete=delete_req)


def get_latest_csv_uri(prefix_uri: str) -> str:
    client = get_s3_client()
    bucket, prefix = parse_s3_uri(prefix_uri)
    paginator = client.get_paginator("list_objects_v2")
    latest_key = None
    latest_blob_key = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            if not key.lower().endswith(".csv"):
                continue
            basename = key.rsplit("/", 1)[-1]
            m = re.search(r"(\d{8})", basename)
            if not m:
                continue
            date_str = m.group(1)
            if latest_key is None or date_str > latest_key or (date_str == latest_key and (latest_blob_key is None or key > latest_blob_key)):
                latest_key = date_str
                latest_blob_key = key
    if latest_blob_key is None:
        raise RuntimeError(f"No CSV with yyyymmdd in name found under {prefix_uri}")
    return f"s3://{bucket}/{latest_blob_key}"


def load_config() -> Dict[str, Any]:
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG
    S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "report-eredi-maggi")
    S3_CONFIG_PREFIX = os.environ.get("S3_CONFIG_PREFIX", "Config/")
    config_uri = _as_s3_prefix_uri(S3_BUCKET_NAME, S3_CONFIG_PREFIX)
    if not config_uri:
        raise RuntimeError("Missing S3_CONFIG_PREFIX environment variable")
    raw_json = read_text_from_s3(config_uri)
    _CONFIG = json.loads(raw_json)
    return _CONFIG


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


def get_source_columns_map(
    config: Dict[str, Any],
    logical_fields: List[str]
) -> Dict[str, str]:
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
