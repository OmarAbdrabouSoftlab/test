
import os
import smtplib

from email.message import EmailMessage



SMTP_HOST = os.environ.get("SMTP_HOST", "email-smtp.eu-central-1.amazonaws.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SMTP_USERNAME = os.environ.get("SMTP_USERNAME")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL")

def get_smtp_connection() -> smtplib.SMTP:
    if not SENDER_EMAIL or not SMTP_USERNAME or not SMTP_PASSWORD:
        raise RuntimeError("Missing SENDER_EMAIL / SMTP_USERNAME / SMTP_PASSWORD")

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USERNAME, SMTP_PASSWORD)
    return server


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
