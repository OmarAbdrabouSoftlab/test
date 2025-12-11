import json
import os

import pandas as pd

from report_schema import read_text_from_s3
from typing import Any, Dict, List, Optional

_CLIENTS_RULES: Optional[List[Dict[str, Any]]] = None


def load_clients_rules() -> List[Dict[str, Any]]:
    global _CLIENTS_RULES
    if _CLIENTS_RULES is not None:
        return _CLIENTS_RULES
    uri = os.environ.get("CLIENTS_S3_URI")
    if not uri:
        _CLIENTS_RULES = []
        return _CLIENTS_RULES
    raw = read_text_from_s3(uri)
    cfg = json.loads(raw)
    rules_cfg = cfg.get("rules", [])
    rules: List[Dict[str, Any]] = []
    for rule in rules_cfg:
        field = rule.get("field")
        report_types = rule.get("report_types") or []
        values = rule.get("values") or []
        if not field or not report_types or not values:
            continue
        rules.append(
            {
                "field": field,
                "report_types": [int(rt) for rt in report_types],
                "values": set(values),
            }
        )
    _CLIENTS_RULES = rules
    return _CLIENTS_RULES


def filter_dataframe_for_report_type(df: pd.DataFrame, report_type: int) -> pd.DataFrame:
    rules = load_clients_rules()
    if not rules:
        return df
    combined_mask = None
    for rule in rules:
        if report_type not in rule["report_types"]:
            continue
        field = rule["field"]
        if field not in df.columns:
            continue
        values = rule["values"]
        mask = df[field].isin(values)
        if combined_mask is None:
            combined_mask = mask
        else:
            combined_mask = combined_mask | mask
    if combined_mask is None:
        return df
    return df[combined_mask].copy()