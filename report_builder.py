import io
import re
import pandas as pd

from typing import Any, Dict, List, Tuple
from openpyxl.styles import Alignment
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


def _sanitize_for_filename(value: Any) -> str:
    s = "" if value is None else str(value)
    s = s.strip()
    if not s:
        return "UNKNOWN"
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^A-Za-z0-9 _.-]", "", s)
    s = s.replace(" ", "_")
    s = s[:120].strip("_")
    return s or "UNKNOWN"


def _merge_vertical_column(ws, df: pd.DataFrame, column_name: str) -> None:
    if column_name not in df.columns:
        return
    if len(df) <= 1:
        return

    col_idx = df.columns.get_loc(column_name) + 1
    start_row = 2
    end_row = start_row + len(df) - 1

    ws.merge_cells(
        start_row=start_row,
        start_column=col_idx,
        end_row=end_row,
        end_column=col_idx,
    )

    cell = ws.cell(row=start_row, column=col_idx)
    cell.alignment = Alignment(vertical="top")


def _to_excel_bytes(
    df: pd.DataFrame,
    sheet_name: str,
    merge_column: str | None = None,
) -> bytes:
    buf = io.BytesIO()

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]

        if merge_column:
            _merge_vertical_column(ws, df, merge_column)

    buf.seek(0)
    return buf.read()


def _prepare_dataframe_for_report_type(
    config: Dict[str, Any],
    report_type: int,
) -> Tuple[pd.DataFrame, str, str]:
    logical_fields = _get_logical_fields_specific_first(config, report_type)
    source_columns_map = get_source_columns_map(config, logical_fields)

    source_name = config["report_types"][str(report_type)].get("source", "main_csv")
    df, input_yyyymmdd = load_source_dataframe_for_report_type(config, source_name, report_type)

    ordered_source_columns = [source_columns_map[lf] for lf in logical_fields]
    missing = [c for c in ordered_source_columns if c not in df.columns]

    if missing:
        raise RuntimeError(
            "Source CSV is missing columns: "
            + ", ".join(missing)
            + "\nCSV columns found: "
            + ", ".join(df.columns.astype(str))
        )

    df = df[ordered_source_columns].copy()

    numeric_fields = get_numeric_logical_fields(config, logical_fields)
    numeric_cols = [source_columns_map[f] for f in numeric_fields]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    roman = REPORT_TYPE_ROMAN[report_type]
    return df, input_yyyymmdd, roman


def build_report_excels_with_metadata(
    report_type: int,
) -> List[Tuple[bytes, str, str, str]]:
    config: Dict[str, Any] = load_config()
    df, input_yyyymmdd, roman = _prepare_dataframe_for_report_type(config, report_type)

    # Types 2–5: single file, no grouping
    if report_type != 1:
        xlsx_bytes = _to_excel_bytes(df, sheet_name=f"tipo_{report_type}")
        return [(xlsx_bytes, input_yyyymmdd, roman, "")]

    # Type 1: split per Ragione Sociale, ordered by Gruppo Merceologico
    rag_soc_col = config["fields"]["ragione_sociale"]["source_column"]
    grp_mer_col = config["fields"]["gruppo_merceologico"]["source_column"]

    if rag_soc_col not in df.columns or grp_mer_col not in df.columns:
        raise RuntimeError(
            f"Type 1 requires columns '{rag_soc_col}' and '{grp_mer_col}'"
        )

    df_sorted = df.sort_values(by=[grp_mer_col], kind="mergesort")

    outputs: List[Tuple[bytes, str, str, str]] = []

    for ragione_sociale, df_group in df_sorted.groupby(
        rag_soc_col, dropna=False, sort=False
    ):
        suffix = _sanitize_for_filename(ragione_sociale)

        xlsx_bytes = _to_excel_bytes(
            df_group,
            sheet_name="tipo_1",
            merge_column=rag_soc_col,
        )

        outputs.append((xlsx_bytes, input_yyyymmdd, roman, suffix))

    if not outputs:
        raise RuntimeError("Type 1: no output generated")

    return outputs
