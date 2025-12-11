import io
import os
import smtplib

import functions_framework
import pandas as pd
from email.message import EmailMessage
from flask import Request, make_response

SMTP_HOST = "smtp.gmail.com"
SMTP_PASSWORD = "rvkq aujj fzeb emhf"
SMTP_PORT = 587
RECIPIENT_EMAIL = "omar.abdrabou97@gmail.com"
SENDER_EMAIL = "omar.abdrabou97@gmail.com"

SHARED_COLS = [
    "FATT_LORDO_SCONTO_CASSA_ANNO_CORR",
    "FATT_LORDO_SCONTO_CASSA_ANNO_PREC",
    "DELTA_FATTURATO",
    "DELTA_FATTURATO_PERC",
    "PORTA_ORD_EVAS_CORR",
]

TYPE_SPECIFIC_COLS = {
    1: ["RAGIONE_SOCIALE", "GRUPPO_MERCEOLOGICO"],
    2: ["GRUPPO_COMMERCIALE", "GRUPPO_MERCEOLOGICO"],
    3: ["CONSORZIO", "GRUPPO_MERCEOLOGICO", "RAGIONE_SOCIALE"],
    4: ["AGENZIA_ANAGRAFICA", "RAGIONE_SOCIALE"],
    5: ["AGENZIA_ANAGRAFICA", "RAGIONE_SOCIALE", "GRUPPO_MERCEOLOGICO"],
}


def get_smtp_connection() -> smtplib.SMTP:
    sender_email = SENDER_EMAIL
    smtp_password = SMTP_PASSWORD

    if not sender_email or not smtp_password:
        raise RuntimeError("Missing SENDER_EMAIL or SMTP_PASSWORD environment variables")

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(sender_email, smtp_password)
    return server


def load_and_filter_dataframe(report_type: int) -> pd.DataFrame:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "test.csv")

    df = pd.read_csv(
        csv_path,
        sep=";",
        decimal=",",
        thousands=".",
        dtype={"report_type": "int64"},
    )

    df_filtered = df[df["report_type"] == report_type].copy()
    if df_filtered.empty:
        raise RuntimeError(f"No rows found for report_type={report_type}")

    specific_cols = TYPE_SPECIFIC_COLS.get(report_type)
    if not specific_cols:
        raise RuntimeError(f"Unsupported report_type={report_type}")

    cols_order = []
    for col in specific_cols + SHARED_COLS + ["report_type"]:
        if col in df_filtered.columns and col not in cols_order:
            cols_order.append(col)

    df_filtered = df_filtered[cols_order]
    return df_filtered


def dataframe_to_excel_bytes(df: pd.DataFrame, report_type: int) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        sheet_name = f"tipo_{report_type}"
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer.read()


def send_report_email(report_type: int, xlsx_bytes: bytes) -> None:
    sender_email = SENDER_EMAIL

    msg = EmailMessage()
    msg["Subject"] = f"Report tipo {report_type}"
    msg["From"] = sender_email
    msg["To"] = RECIPIENT_EMAIL
    msg.set_content(f"Report tipo {report_type} in allegato.")

    filename = f"report_tipo_{report_type}.xlsx"
    msg.add_attachment(
        xlsx_bytes,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )

    server = get_smtp_connection()
    try:
        server.send_message(msg)
    finally:
        server.quit()


@functions_framework.http
def generate_report(request: Request):
    try:
        if request.method == "GET":
            report_type_raw = request.args.get("report_type")
        else:
            body = request.get_json(silent=True) or {}
            report_type_raw = body.get("report_type")

        if not report_type_raw:
            return make_response(("Missing 'report_type' parameter", 400))

        try:
            report_type = int(report_type_raw)
        except ValueError:
            return make_response(("'report_type' must be an integer", 400))

        df = load_and_filter_dataframe(report_type)
        xlsx_bytes = dataframe_to_excel_bytes(df, report_type)
        send_report_email(report_type, xlsx_bytes)

        return make_response((f"Report tipo {report_type} generated and emailed.", 200))

    except Exception as exc:
        return make_response((f"Internal error: {exc}", 500))
