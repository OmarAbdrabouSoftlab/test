import io
import pandas as pd

from typing import Any, Dict, List
from clients_rules import filter_dataframe_for_report_type
from csv_loader import load_source_dataframe
from report_schema import (
    get_logical_fields_for_type,
    get_numeric_logical_fields,
    get_source_columns_map,
    load_config,
)



def build_report_excel(report_type: int) -> bytes:

    config: Dict[str, Any] = load_config()
    logical_fields: List[str] = get_logical_fields_for_type(config, report_type)
    source_columns_map: Dict[str, str] = get_source_columns_map(config, logical_fields)

    report_types = config["report_types"]
    source_name = report_types[str(report_type)].get("source", "main_csv")

    df = load_source_dataframe(config, source_name)
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
    return output.read()
