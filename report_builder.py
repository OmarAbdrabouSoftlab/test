import io
import re
import pandas as pd

from typing import Any, Dict, List, Tuple

from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

from csv_loader import REPORT_TYPE_ROMAN, load_source_dataframe_for_report_type
from report_schema import get_numeric_logical_fields, get_source_columns_map, load_config


_FORCE_NUMERIC_FORMAT_COLS = {
    "Fatturato_lordo_sconto_cassa_25",
    "Fatturato_lordo_sconto_cassa_24",
    "Delta_25vs24",
}

# Excel loses integer precision beyond ~15 digits. Option A: store those as text.
_TEXT_THRESHOLD_ABS = 1e15

# Keep your requested formatting for "normal-sized" values
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


def _coerce_nullable_int_series(s: pd.Series) -> pd.Series:
    """
    Convert to pandas nullable integer (Int64) without going through float.
    Handles strings, blanks, NaN safely.
    """
    # Normalize common empties
    s2 = s.replace(r"^\s*$", pd.NA, regex=True)

    # to_numeric can produce floats if there are NaNs; forcing Int64 afterwards
    # ensures we keep exact integers (or <NA>) and do not keep float values.
    out = pd.to_numeric(s2, errors="coerce")

    # If out is float because of NaNs, conversion to Int64 will keep exact ints and <NA>.
    # NOTE: if there are true decimals, they will become <NA> here. That is intentional for these columns.
    return out.astype("Int64")


def _apply_client_numeric_formatting(ws, df: pd.DataFrame) -> None:
    """
    Option A:
    - For very large integer magnitudes (>= 1e15): write as TEXT from the df integer value.
    - Otherwise: keep numeric with a normal number format to avoid scientific notation.
    """
    target_cols = [c for c in df.columns if c in _FORCE_NUMERIC_FORMAT_COLS]
    if not target_cols or df.empty:
        return

    _ensure_unique_columns(df)

    start_row = 2
    end_row = start_row + len(df) - 1

    # df is written without index, so row r corresponds to df row (r - 2)
    for col_name in target_cols:
        col_idx_1based = _get_col_idx_1based(df, col_name)

        for r in range(start_row, end_row + 1):
            df_row = r - start_row  # 0-based within this sheet
            v = df.iat[df_row, df.columns.get_loc(col_name)]

            cell = ws.cell(row=r, column=col_idx_1based)

            if pd.isna(v):
                continue

            # v is Int64 scalar -> cast to Python int safely
            iv = int(v)

            if abs(iv) >= _TEXT_THRESHOLD_ABS:
                # Preserve exact digits: write as text
                cell.value = str(iv)
                cell.number_format = "@"
            else:
                # Keep numeric and formatted (prevents E+ for "normal sized" values)
                cell.value = float(iv)
                cell.number_format = _NUM_FORMAT_2DP

        # Make the column readable
        col_letter = get_column_letter(col_idx_1based)
        current_width = ws.column_dimensions[col_letter].width
        ws.column_dimensions[col_letter].width = max(current_width or 0, 18)


def _to_excel_bytes(
    df: pd.DataFrame,
    sheet_name: str,
    merge_column: str | None = None,
) -> bytes:
    _ensure_unique_columns(df)

    # Critical: make row mapping stable for formatting logic (r-2 indexing)
    df_out = df.reset_index(drop=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]

        if merge_column:
            _merge_vertical_column(ws, df_out, merge_column)

        _apply_client_numeric_formatting(ws, df_out)

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
    _ensure_unique_columns(df)

    numeric_fields = get_numeric_logical_fields(config, logical_fields)
    numeric_cols = [source_columns_map[f] for f in numeric_fields]

    # Split numeric conversion:
    # - "High-risk" cols: parse as nullable Int64 to preserve exact digits.
    # - Other numeric cols: normal numeric parsing is fine.
    special_cols = [c for c in numeric_cols if c in _FORCE_NUMERIC_FORMAT_COLS]
    normal_numeric_cols = [c for c in numeric_cols if c not in _FORCE_NUMERIC_FORMAT_COLS]

    for col in normal_numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in special_cols:
        df[col] = _coerce_nullable_int_series(df[col])

    roman = REPORT_TYPE_ROMAN[report_type]
    return df, input_yyyymmdd, roman


def build_report_excels_with_metadata(
    report_type: int,
) -> List[Tuple[bytes, str, str, str]]:
    config: Dict[str, Any] = load_config()
    df, input_yyyymmdd, roman = _prepare_dataframe_for_report_type(config, report_type)

    if report_type != 1:
        xlsx_bytes = _to_excel_bytes(df, sheet_name=f"tipo_{report_type}")
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
            merge_column=rag_soc_col,
        )

        outputs.append((xlsx_bytes, input_yyyymmdd, roman, suffix))

    if not outputs:
        raise RuntimeError("Type 1: no output generated")

    return outputs