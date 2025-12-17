import datetime
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import functions_framework
from flask import Request, make_response

from mail_handling import load_client_emails, send_report_email
from report_builder import build_report_excels_with_metadata
from report_schema import (
    delete_s3_prefix,
    load_config, write_bytes_to_s3,
    output_today_prefix_uri,
    parse_report_type_key,
    output_report_type_prefix_uri,
    sanitize_for_filename,
    report_type_keys_for_tipo_report,
    report_type_keys_for_tipo_report,
    match_produced_files_for_client
)

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
S3_OUTPUT_PREFIX = os.environ.get("S3_OUTPUT_PREFIX")


@functions_framework.http
def generate_report(request: Request):
    try:
        if not S3_BUCKET_NAME:
            return make_response(("Missing S3_BUCKET_NAME environment variable", 500))

        config = load_config()
        config_report_type_keys = list((config.get("report_types") or {}).keys())

        today = datetime.date.today().strftime("%Y-%m-%d")
        output_today_uri = output_today_prefix_uri(S3_BUCKET_NAME, S3_OUTPUT_PREFIX or "Output/", today)

        delete_s3_prefix(output_today_uri)

        written: List[str] = []
        skipped: List[str] = []
        failed: List[str] = []

        report_meta: Dict[str, Tuple[str, str]] = {}
        produced_files: Dict[Tuple[str, str], bytes] = {}

        for report_type_key in config_report_type_keys:
            try:
                outputs = build_report_excels_with_metadata(report_type_key)
                output_report_type_uri = output_report_type_prefix_uri(output_today_uri, report_type_key)

                _, subtype, _ = parse_report_type_key(report_type_key)

                for xlsx_bytes, input_yyyymmdd, roman, suffix in outputs:
                    report_meta.setdefault(report_type_key, (roman, input_yyyymmdd))

                    prefix_token = f"{roman}_{subtype}" if subtype else roman
                    file_suffix = f"_{suffix}" if suffix else ""
                    output_filename = f"FT_BC_OC_REPORT_{prefix_token}_{input_yyyymmdd}{file_suffix}.xlsx"

                    produced_files[(report_type_key, suffix or "")] = xlsx_bytes

                    output_uri = f"{output_report_type_uri}{output_filename}"

                    write_bytes_to_s3(
                        output_uri,
                        xlsx_bytes,
                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                    written.append(output_uri)

            except Exception as exc:
                msg = str(exc)
                if "No matching CSV found for report_type" in msg:
                    skipped.append(f"type {report_type_key}: {msg}")
                else:
                    failed.append(f"type {report_type_key}: {repr(exc)}")

        mail_sent: List[str] = []
        mail_failed: List[str] = []

        try:
            emails_map = load_client_emails()

            for tipo_report, clients in emails_map.items():
                target_report_type_keys = report_type_keys_for_tipo_report(tipo_report, config_report_type_keys)
                if not target_report_type_keys:
                    continue

                for client_id, recipients in clients.items():
                    if not recipients:
                        continue

                    client_suffix = sanitize_for_filename(client_id)

                    for report_type_key in target_report_type_keys:
                        if report_type_key not in report_meta:
                            continue

                        roman, input_yyyymmdd = report_meta[report_type_key]

                        matched_files = match_produced_files_for_client(
                            produced_files=produced_files,
                            report_type_key=report_type_key,
                            client_suffix=client_suffix,
                        )

                        if not matched_files:
                            mail_failed.append(
                                f"Missing output for tipo_report={tipo_report}, report_type={report_type_key}, client_id={client_id}"
                            )
                            continue

                        for suffix, xlsx_bytes in matched_files:
                            attachment_filename = f"FT_BC_OC_REPORT_{roman}_{input_yyyymmdd}_{suffix}.xlsx"
                            subject = f"FT_BC_OC_REPORT {report_type_key} {input_yyyymmdd}"
                            body = f"In allegato il report {report_type_key} del {input_yyyymmdd}."

                            try:
                                send_report_email(
                                    recipients=recipients,
                                    subject=subject,
                                    body=body,
                                    attachment_filename=attachment_filename,
                                    xlsx_bytes=xlsx_bytes,
                                )
                                mail_sent.append(
                                    f"sent tipo_report={tipo_report} report_type={report_type_key} client_id={client_id} suffix={suffix} to {', '.join(recipients)}"
                                )
                            except Exception as exc:
                                mail_failed.append(
                                    f"Email failed tipo_report={tipo_report} report_type={report_type_key} client_id={client_id} suffix={suffix}: {repr(exc)}"
                                )

        except Exception as exc:
            mail_failed.append(f"Email phase failed to start: {repr(exc)}")

        lines: List[str] = []
        lines.append(f"written: {len(written)}")
        lines.append(f"skipped: {len(skipped)}")
        lines.extend(skipped)
        lines.append(f"failed: {len(failed)}")
        lines.extend(failed)
        lines.append(f"mail_sent: {len(mail_sent)}")
        lines.append(f"mail_failed: {len(mail_failed)}")
        lines.extend(mail_failed)

        status = 200 if len(written) > 0 and len(failed) == 0 else 500 if len(written) == 0 else 207
        return make_response(("\n".join(lines), status))

    except Exception as exc:
        return make_response((f"Internal error: {exc}", 500))