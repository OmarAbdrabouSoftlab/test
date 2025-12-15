import io
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd
from typing import Any, Dict, List, Tuple

from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

from csv_loader import REPORT_TYPE_ROMAN, load_source_dataframe_for_report_type
from report_schema import get_source_columns_map, load_config


# Columns that must be rendered as NUMBERS in Excel with 2 decimals, using '.' as decimal separator in CSV.
_CLIENT_NUMERIC_2DP_COLS = {
    "DeltaPerc_25vs24",
    "ORDINATO_INEVASO_2025",
    "Fatturato_lordo_sconto_cassa_25",
    "Fatturato_lordo_sconto_cassa_24",
    "Delta_25vs24",
}

_NUM_FORMAT_2DP = "#,##0.00"
_COL_WIDTH_NUM = 18
_TOTAL_LABEL = "TOTALE"


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


def _merge_vertical_column(
    ws,
    df: pd.DataFrame,
    column_name: str,
    *,
    exclude_last_row: bool = False,
) -> None:
    """
    Merge an entire column vertically from first data row to last data row.
    If exclude_last_row=True, the last dataframe row is excluded (useful for totals row).
    """
    if column_name not in df.columns:
        return
    if len(df) <= 1:
        return

    _ensure_unique_columns(df)

    col_idx = _get_col_idx_1based(df, column_name)
    start_row = 2

    data_len = len(df) - (1 if exclude_last_row else 0)
    if data_len <= 1:
        return

    end_row = start_row + data_len - 1

    ws.merge_cells(
        start_row=start_row,
        start_column=col_idx,
        end_row=end_row,
        end_column=col_idx,
    )

    cell = ws.cell(row=start_row, column=col_idx)
    cell.alignment = Alignment(vertical="top")


def _parse_decimal_2dp(value: Any) -> Decimal | None:
    """
    Parse a CSV string ('.' decimal separator) into a Decimal rounded to 2 dp.
    Returns None for blanks/NaN/unparseable.
    """
    if value is None:
        return None
    if pd.isna(value):
        return None

    s = str(value).strip()
    if not s:
        return None

    try:
        d = Decimal(s)
    except (InvalidOperation, ValueError):
        return None

    return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _coerce_numeric_2dp_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the configured client numeric columns to Decimal(2dp) where present.
    Leaves other columns untouched.
    """
    for col in df.columns:
        if col in _CLIENT_NUMERIC_2DP_COLS:
            df[col] = df[col].map(_parse_decimal_2dp)
    return df


def _append_totals_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Appends a totals row at the bottom, summing only _CLIENT_NUMERIC_2DP_COLS.
    Non-numeric columns are left blank, except one label column gets 'TOTALE'.
    """
    if df.empty:
        return df

    totals: Dict[str, Any] = {c: None for c in df.columns}

    # Pick a label column (first non-numeric column if possible, else first column).
    label_col = next((c for c in df.columns if c not in _CLIENT_NUMERIC_2DP_COLS), df.columns[0])
    totals[label_col] = _TOTAL_LABEL

    for col in df.columns:
        if col not in _CLIENT_NUMERIC_2DP_COLS:
            continue

        s = df[col]
        total = Decimal("0.00")
        for v in s:
            if v is None or pd.isna(v):
                continue
            # v should be Decimal already; but be safe.
            if isinstance(v, Decimal):
                total += v
            else:
                parsed = _parse_decimal_2dp(v)
                if parsed is not None:
                    total += parsed

        totals[col] = total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    return pd.concat([df, pd.DataFrame([totals])], ignore_index=True)


def _apply_client_numeric_formatting(ws, df: pd.DataFrame) -> None:
    """
    Applies Excel number formatting to the configured numeric columns.
    Values are assumed to already be numeric (Decimal) or blank.
    """
    target_cols = [c for c in df.columns if c in _CLIENT_NUMERIC_2DP_COLS]
    if not target_cols or df.empty:
        return

    _ensure_unique_columns(df)

    start_row = 2
    end_row = start_row + len(df) - 1

    for col_name in target_cols:
        col_idx_1based = _get_col_idx_1based(df, col_name)

        for r in range(start_row, end_row + 1):
            cell = ws.cell(row=r, column=col_idx_1based)
            if cell.value is None or cell.value == "":
                continue
            cell.number_format = _NUM_FORMAT_2DP

        col_letter = get_column_letter(col_idx_1based)
        current_width = ws.column_dimensions[col_letter].width
        ws.column_dimensions[col_letter].width = max(current_width or 0, _COL_WIDTH_NUM)


def _to_excel_bytes(
    df: pd.DataFrame,
    sheet_name: str,
    merge_column: str | None = None,
    *,
    add_totals_row: bool = False,
) -> bytes:
    _ensure_unique_columns(df)

    df_out = df.reset_index(drop=True)
    if add_totals_row:
        df_out = _append_totals_row(df_out)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]

        if merge_column:
            _merge_vertical_column(
                ws,
                df_out,
                merge_column,
                exclude_last_row=add_totals_row,
            )

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

    df = _coerce_numeric_2dp_columns(df)

    roman = REPORT_TYPE_ROMAN[report_type]
    return df, input_yyyymmdd, roman


def build_report_excels_with_metadata(
    report_type: int,
) -> List[Tuple[bytes, str, str, str]]:
    config: Dict[str, Any] = load_config()
    df, input_yyyymmdd, roman = _prepare_dataframe_for_report_type(config, report_type)

    # Add totals row to all report types (including type 1 group files).
    add_totals = True

    if report_type != 1:
        xlsx_bytes = _to_excel_bytes(
            df,
            sheet_name=f"tipo_{report_type}",
            add_totals_row=add_totals,
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
            merge_column=rag_soc_col,
            add_totals_row=add_totals,
        )

        outputs.append((xlsx_bytes, input_yyyymmdd, roman, suffix))

    if not outputs:
        raise RuntimeError("Type 1: no output generated")

    return outputs