import io
import re
from typing import Any, Dict, List, Tuple

import pandas as pd
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

from csv_loader import REPORT_TYPE_ROMAN, load_source_dataframe_for_report_type
from report_schema import get_numeric_logical_fields, get_source_columns_map, load_config


_CLIENT_NUMERIC_2DP_COLS = {
    "Fatturato_lordo_sconto_cassa_CY",
    "Fatturato_lordo_sconto_cassa_PY",
    "Delta_CYvsPY",
    "DeltaPerc_CYvsPY",
    "ORDINATO_INEVASO_CY",
}

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


def _parse_numeric_cell(value: Any) -> Any:
    """
    Parse numbers stored as strings, supporting:
    - EU style: 1.234.567,89
    - plain: 1234.56
    - multiple-dot thousands: 7.298.877.514.822.880 (treated as integer)
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return pd.NA
    if isinstance(value, (int, float)):
        return value

    s = str(value).strip()

    # EU style: '.' thousands + ',' decimal
    if "," in s and "." in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
        return pd.to_numeric(s, errors="coerce")

    # Multiple dots, no comma: treat as thousands separators (integer-like)
    if s.count(".") >= 2 and "," not in s:
        s = s.replace(".", "")
        return pd.to_numeric(s, errors="coerce")

    # Standard: 1234.56 or 1234
    return pd.to_numeric(s, errors="coerce")


def _coerce_numeric_columns(df: pd.DataFrame, numeric_cols: List[str]) -> pd.DataFrame:
    if not numeric_cols or df.empty:
        return df

    out = df.copy()
    for col in numeric_cols:
        if col not in out.columns:
            continue
        out[col] = out[col].map(_parse_numeric_cell)
    return out


def _append_totals_row(df: pd.DataFrame, numeric_cols: List[str], label_col: str | None = None) -> pd.DataFrame:
    if df.empty or not numeric_cols:
        return df

    totals = {c: pd.NA for c in df.columns}
    for c in numeric_cols:
        if c in df.columns:
            totals[c] = pd.to_numeric(df[c], errors="coerce").sum(skipna=True)

    if label_col and label_col in df.columns:
        totals[label_col] = "TOTALE"
    elif len(df.columns) > 0:
        totals[df.columns[0]] = "TOTALE"

    return pd.concat([df, pd.DataFrame([totals])], ignore_index=True)


def _apply_excel_number_formatting(ws, df: pd.DataFrame, numeric_cols: List[str]) -> None:
    if df.empty or not numeric_cols:
        return

    _ensure_unique_columns(df)

    start_row = 2
    end_row = start_row + len(df) - 1

    for col_name in numeric_cols:
        if col_name not in df.columns:
            continue

        col_idx_1based = _get_col_idx_1based(df, col_name)

        for r in range(start_row, end_row + 1):
            df_row = r - start_row
            v = df.iat[df_row, df.columns.get_loc(col_name)]
            cell = ws.cell(row=r, column=col_idx_1based)

            if pd.isna(v):
                continue

            cell.value = float(v)
            cell.number_format = _NUM_FORMAT_2DP

        col_letter = get_column_letter(col_idx_1based)
        current_width = ws.column_dimensions[col_letter].width
        ws.column_dimensions[col_letter].width = max(current_width or 0, 18)


def _to_excel_bytes(
    df: pd.DataFrame,
    sheet_name: str,
    merge_columns: List[str] | None = None,
    numeric_cols: List[str] | None = None,
) -> bytes:
    _ensure_unique_columns(df)

    df_out = df.reset_index(drop=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]

        for c in (merge_columns or []):
            _merge_vertical_column(ws, df_out, c)

        _apply_excel_number_formatting(ws, df_out, numeric_cols or [])

    buf.seek(0)
    return buf.read()


def _grouping_columns_for_report_type(config: Dict[str, Any], report_type: int) -> List[str]:
    fields = config["fields"]

    def sc(logical_name: str) -> str:
        return fields[logical_name]["source_column"]

    if report_type == 1:
        return [sc("ragione_sociale")]
    if report_type == 3:
        return [sc("consorzio"), sc("ragione_sociale")]
    if report_type == 4:
        return [sc("agenzia_anagrafica")]
    if report_type == 5:
        return [sc("agenzia_anagrafica"), sc("ragione_sociale")]
    return []


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

    numeric_fields = get_numeric_logical_fields(config, logical_fields)
    schema_numeric_cols = [source_columns_map[f] for f in numeric_fields]

    numeric_cols = [c for c in schema_numeric_cols if c in _CLIENT_NUMERIC_2DP_COLS]

    df = _coerce_numeric_columns(df, numeric_cols)

    roman = REPORT_TYPE_ROMAN[report_type]
    return df, input_yyyymmdd, roman, numeric_cols


def build_report_excels_with_metadata(
    report_type: int,
) -> List[Tuple[bytes, str, str, str]]:
    config: Dict[str, Any] = load_config()
    df, input_yyyymmdd, roman, numeric_cols = _prepare_dataframe_for_report_type(config, report_type)

    group_cols = _grouping_columns_for_report_type(config, report_type)

    for gc in group_cols:
        if gc not in df.columns:
            raise RuntimeError(
                f"Report type {report_type} requires grouping column '{gc}' but it is missing"
            )

    outputs: List[Tuple[bytes, str, str, str]] = []

    if not group_cols:
        df_out = _append_totals_row(
            df,
            numeric_cols=numeric_cols,
            label_col=None,
        )

        xlsx_bytes = _to_excel_bytes(
            df_out,
            sheet_name=f"tipo_{report_type}",
            merge_columns=[],
            numeric_cols=numeric_cols,
        )

        return [(xlsx_bytes, input_yyyymmdd, roman, "")]

    df_base = df.reset_index(drop=True)

    for keys, df_group in df_base.groupby(group_cols, dropna=False, sort=False):
        if not isinstance(keys, tuple):
            keys = (keys,)

        suffix = "__".join(_sanitize_for_filename(k) for k in keys)

        label_col = group_cols[-1]

        df_out = _append_totals_row(
            df_group,
            numeric_cols=numeric_cols,
            label_col=label_col,
        )

        xlsx_bytes = _to_excel_bytes(
            df_out,
            sheet_name=f"tipo_{report_type}",
            merge_columns=group_cols,
            numeric_cols=numeric_cols,
        )

        outputs.append((xlsx_bytes, input_yyyymmdd, roman, suffix))

    if not outputs:
        raise RuntimeError(f"Type {report_type}: no output generated")

    return outputs