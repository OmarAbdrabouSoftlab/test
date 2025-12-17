import io
import os
import smtplib
import pandas as pd

from email.message import EmailMessage
from typing import Dict, List
from report_schema import _as_s3_prefix_uri, read_bytes_from_s3


_CLIENT_EMAILS_FILENAME = "client_emails.xlsx"
_CLIENT_EMAILS_REQUIRED_COLS = {"ID_CLIENTE", "EMAIL_ASSOCIATE", "TIPO_REPORT"}


def load_client_emails() -> Dict[str, Dict[str, List[str]]]:
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

    df = df[list(_CLIENT_EMAILS_REQUIRED_COLS)].copy()
    df["ID_CLIENTE"] = df["ID_CLIENTE"].astype("string").fillna("").str.strip()
    df["EMAIL_ASSOCIATE"] = df["EMAIL_ASSOCIATE"].astype("string").fillna("").str.strip()
    df["TIPO_REPORT"] = df["TIPO_REPORT"].astype("string").fillna("").str.strip()

    mapping: Dict[str, Dict[str, List[str]]] = {}
    for _, row in df.iterrows():
        client_id = (row["ID_CLIENTE"] or "").strip()
        emails_raw = (row["EMAIL_ASSOCIATE"] or "").strip()
        tipo_report = (row["TIPO_REPORT"] or "").strip()

        if not client_id or not tipo_report:
            continue

        emails = [e.strip() for e in emails_raw.split(",") if e and e.strip()]
        if not emails:
            continue

        bucket_map = mapping.setdefault(tipo_report, {})
        lst = bucket_map.setdefault(client_id, [])
        for e in emails:
            if e not in lst:
                lst.append(e)

    return mapping


def send_report_email(
    *,
    recipients: List[str],
    subject: str,
    body: str,
    attachment_filename: str,
    xlsx_bytes: bytes,
) -> None:
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port_raw = os.environ.get("SMTP_PORT")
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASSWORD")
    from_email = os.environ.get("SENDER_EMAIL")

    if not smtp_host:
        raise RuntimeError("Missing SMTP_HOST environment variable")
    if not from_email:
        raise RuntimeError("Missing SENDER_EMAIL environment variable")
    if not recipients:
        raise RuntimeError("No recipients provided")

    try:
        smtp_port = int(str(smtp_port_raw).strip())
    except ValueError as exc:
        raise RuntimeError(f"Invalid SMTP_PORT: {smtp_port_raw}") from exc

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)

    msg.add_attachment(
        xlsx_bytes,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=attachment_filename,
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)
        server.send_message(msg)