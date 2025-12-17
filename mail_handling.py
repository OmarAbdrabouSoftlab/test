
import os
import io
import smtplib
import pandas as pd

from email.message import EmailMessage
from typing import Dict, List
from report_schema import _as_s3_prefix_uri, read_bytes_from_s3


SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = os.environ.get("SMTP_PORT")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SMTP_USERNAME = os.environ.get("SMTP_USERNAME")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
# RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL")
RECIPIENT_EMAIL = os.environ.get("omar.abdrabou@soft.it")


_CLIENT_EMAILS_FILENAME = os.environ.get("S3_CLIENT_CONFIG","client_emails.xlsx")
_CLIENT_EMAILS_REQUIRED_COLS = {"ID_CLIENTE", "EMAIL_ASSOCIATE", "TIPO_REPORT"}

def get_smtp_connection() -> smtplib.SMTP:
    if not SENDER_EMAIL or not SMTP_USERNAME or not SMTP_PASSWORD:
        raise RuntimeError("Missing SENDER_EMAIL / SMTP_USERNAME / SMTP_PASSWORD")

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USERNAME, SMTP_PASSWORD)
    return server


def load_client_emails() -> Dict[str, Dict[str, List[str]]]:
    mapping: Dict[str, Dict[str, List[str]]] = {}

    bucket = os.environ.get("S3_BUCKET_NAME")
    prefix = os.environ.get("S3_CONFIG_PREFIX", "Config/")
    if not bucket:
        raise RuntimeError("Missing S3_BUCKET_NAME environment variable")

    prefix_uri = _as_s3_prefix_uri(bucket, prefix)

    xlsx_uri = f"{prefix_uri}{_CLIENT_EMAILS_FILENAME}"
    data = read_bytes_from_s3(xlsx_uri)

    df = pd.read_excel(io.BytesIO(data), dtype="string")
    missing = _CLIENT_EMAILS_REQUIRED_COLS - set(df.columns.astype(str))
    if missing:
        raise RuntimeError(
            f"client_emails.xlsx is missing columns: {', '.join(sorted(missing))}. "
            f"Found: {', '.join(df.columns.astype(str))}"
        )

    df = df[["ID_CLIENTE", "EMAIL_ASSOCIATE", "TIPO_REPORT"]].copy()
    df["ID_CLIENTE"] = df["ID_CLIENTE"].astype("string").fillna("").str.strip()
    df["EMAIL_ASSOCIATE"] = df["EMAIL_ASSOCIATE"].astype("string").fillna("").str.strip()
    df["TIPO_REPORT"] = df["TIPO_REPORT"].astype("string").fillna("").str.strip()

    for _, row in df.iterrows():
        client_id = str(row["ID_CLIENTE"]).strip().replace(" ", "_")
        report_type = str(row["TIPO_REPORT"]).strip()
        emails_raw = str(row["EMAIL_ASSOCIATE"]).strip()

        if not client_id or not report_type:
            continue

        emails = [e.strip() for e in emails_raw.split(",") if e and e.strip()]
        if not emails:
            continue

        mapping.setdefault(report_type, {}).setdefault(client_id, [])
        dest = mapping[report_type][client_id]

        for e in emails:
            if e not in dest:
                dest.append(e)

    return mapping


def send_report_email(report_type: int, xlsx_bytes: bytes) -> None:
    if not SENDER_EMAIL:
        raise RuntimeError("SENDER_EMAIL environment variable is not set")

    msg = EmailMessage()
    msg["Subject"] = f"Report tipo {report_type}"
    msg["From"] = SENDER_EMAIL
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
