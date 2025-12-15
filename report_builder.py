import io
import re
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

from csv_loader import REPORT_TYPE_ROMAN, load_source_dataframe_for_report_type
from report_schema import get_numeric_logical_fields, get_source_columns_map, load_config


# Excel number format to avoid scientific notation for normal numeric columns
_NUM_FORMAT_2DP = "#,##0.00"


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


def _ensure_unique_columns(df: pd.DataFrame) -> None:
    if not df.columns.is_unique:
        dupes = df.columns[df.columns.duplicated()].astype(str).tolist()
        raise ValueError(f"Duplicate column names detected: {dupes}")


def _get_col_idx_1based(df: pd.DataFrame, column_name: str) -> int:
    loc = df.columns.get_loc(column_name)
    if not isinstance(loc, int):
        raise ValueError(
            f"Expected unique column match for '{column_name}', got {type(loc).__name__}. "
            "This usually indicates duplicate column names."
        )
    return loc + 1


def _merge_vertical_column(ws, df: pd.DataFrame, column_name: str) -> None:
    if column_name not in df.columns:
        return
    if len(df) <= 1:
        return

    _ensure_unique_columns(df)

    col_idx = _get_col_idx_1based(df, column_name)
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


def _parse_number_loose(raw: Any) -> Optional[float]:
    """
    Robust numeric parser for strings coming from CSV (read as text).
    Handles:
      - Italian style: 1.234.567,89
      - US style:     1,234,567.89
      - Plain:        12345 / 12345.67 / 12345,67
    Returns float or None if not parseable.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None

    s = str(raw).strip()
    if not s:
        return None

    # normalize spaces
    s = s.replace(" ", "")

    # If both separators are present, decide decimal by last occurrence.
    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        decimal_sep = "," if last_comma > last_dot else "."
        thousands_sep = "." if decimal_sep == "," else ","
        s = s.replace(thousands_sep, "")
        s = s.replace(decimal_sep, ".")
    else:
        # Only one of them (or none)
        if "," in s:
            # Heuristic: if last group is 1-2 digits, treat comma as decimal, else thousands.
            parts = s.split(",")
            if len(parts) == 2 and len(parts[1]) in (1, 2):
                s = s.replace(".", "")  # treat dots as thousands if present
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "." in s:
            parts = s.split(".")
            if len(parts) == 2 and len(parts[1]) in (1, 2):
                # decimal dot
                s = s.replace(",", "")  # treat commas as thousands if present
            else:
                # thousands dots
                s = s.replace(".", "")

    try:
        return float(s)
    except ValueError:
        return None


def _apply_numeric_column_formatting(ws, df_out: pd.DataFrame, numeric_columns: List[str]) -> None:
    """
    Formats only numeric columns (as per schema type=number) to avoid scientific notation.
    Assumes df_out already contains numeric values in those columns.
    """
    if df_out.empty or not numeric_columns:
        return

    _ensure_unique_columns(df_out)

    start_row = 2
    end_row = start_row + len(df_out) - 1

    for col_name in numeric_columns:
        if col_name not in df_out.columns:
            continue

        col_idx_1based = _get_col_idx_1based(df_out, col_name)

        for r in range(start_row, end_row + 1):
            cell = ws.cell(row=r, column=col_idx_1based)
            # If it's numeric, enforce a readable numeric format
            if isinstance(cell.value, (int, float)) and cell.value is not None:
                cell.number_format = _NUM_FORMAT_2DP

        # Make the column readable
        col_letter = get_column_letter(col_idx_1based)
        current_width = ws.column_dimensions[col_letter].width
        ws.column_dimensions[col_letter].width = max(current_width or 0, 18)


def _to_excel_bytes(
    df: pd.DataFrame,
    sheet_name: str,
    numeric_columns: List[str],
    merge_column: str | None = None,
) -> bytes:
    _ensure_unique_columns(df)
    df_out = df.reset_index(drop=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]

        if merge_column:
            _merge_vertical_column(ws, df_out, merge_column)

        _apply_numeric_column_formatting(ws, df_out, numeric_columns)

    buf.seek(0)
    return buf.read()


def _prepare_dataframe_for_report_type(
    config: Dict[str, Any],
    report_type: int,
) -> Tuple[pd.DataFrame, str, str, List[str]]:
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
    _ensure_unique_columns(df)

    # Convert ONLY fields declared as numeric in the schema.
    numeric_logical_fields = get_numeric_logical_fields(config, logical_fields)
    numeric_source_columns = [source_columns_map[lf] for lf in numeric_logical_fields]

    for col in numeric_source_columns:
        # We read everything as string: parse robustly here.
        df[col] = df[col].map(_parse_number_loose)

    roman = REPORT_TYPE_ROMAN[report_type]
    return df, input_yyyymmdd, roman, numeric_source_columns


def build_report_excels_with_metadata(report_type: int) -> List[Tuple[bytes, str, str, str]]:
    config: Dict[str, Any] = load_config()
    df, input_yyyymmdd, roman, numeric_source_columns = _prepare_dataframe_for_report_type(config, report_type)

    if report_type != 1:
        xlsx_bytes = _to_excel_bytes(
            df,
            sheet_name=f"tipo_{report_type}",
            numeric_columns=numeric_source_columns,
        )
        return [(xlsx_bytes, input_yyyymmdd, roman, "")]

    rag_soc_col = config["fields"]["ragione_sociale"]["source_column"]
    grp_mer_col = config["fields"]["gruppo_merceologico"]["source_column"]

    if rag_soc_col not in df.columns or grp_mer_col not in df.columns:
        raise RuntimeError(f"Type 1 requires columns '{rag_soc_col}' and '{grp_mer_col}'")

    df_sorted = df.sort_values(by=[grp_mer_col], kind="mergesort")

    outputs: List[Tuple[bytes, str, str, str]] = []
    for ragione_sociale, df_group in df_sorted.groupby(rag_soc_col, dropna=False, sort=False):
        suffix = _sanitize_for_filename(ragione_sociale)

        xlsx_bytes = _to_excel_bytes(
            df_group,
            sheet_name="tipo_1",
            numeric_columns=numeric_source_columns,
            merge_column=rag_soc_col,
        )

        outputs.append((xlsx_bytes, input_yyyymmdd, roman, suffix))

    if not outputs:
        raise RuntimeError("Type 1: no output generated")

    return outputs