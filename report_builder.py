import io

import pandas as pd
from typing import Any, Dict, List, Tuple

from csv_loader import REPORT_TYPE_ROMAN, load_source_dataframe_for_report_type
from report_schema import get_numeric_logical_fields, get_source_columns_map, load_config


def _get_logical_fields_specific_first(config: Dict[str, Any], report_type: int) -> List[str]:
    report_types = config["report_types"]
    type_key = str(report_type)
    if type_key not in report_types:
        raise KeyError(f"Unknown report_type: {report_type}")

    specific = report_types[type_key]["specific_fields"]
    shared = config["shared_fields"]

    result: List[str] = []
    seen = set()

    for field in list(specific) + list(shared):
        if field not in seen:
            seen.add(field)
            result.append(field)

    return result


def build_report_excel(report_type: int) -> bytes:
    xlsx_bytes, _, _ = build_report_excel_with_metadata(report_type)
    return xlsx_bytes


def build_report_excel_with_metadata(report_type: int) -> Tuple[bytes, str, str]:
    config: Dict[str, Any] = load_config()

    logical_fields = _get_logical_fields_specific_first(config, report_type)
    source_columns_map = get_source_columns_map(config, logical_fields)

    report_types = config["report_types"]
    source_name = report_types[str(report_type)].get("source", "main_csv")

    df, input_yyyymmdd = load_source_dataframe_for_report_type(config, source_name, report_type)

    ordered_source_columns = [source_columns_map[lf] for lf in logical_fields]
    missing = [col for col in ordered_source_columns if col not in df.columns]
    if missing:
        raise RuntimeError(f"Source CSV is missing columns: {', '.join(missing)}")

    df = df[ordered_source_columns].copy()

    numeric_logical_fields = get_numeric_logical_fields(config, logical_fields)
    numeric_source_columns = [source_columns_map[lf] for lf in numeric_logical_fields]

    for col in numeric_source_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=f"tipo_{report_type}")
    output.seek(0)

    roman = REPORT_TYPE_ROMAN[report_type]
    return output.read(), input_yyyymmdd, roman