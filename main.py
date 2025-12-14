import datetime
import os
import functions_framework

from flask import Request, make_response
from mail_handling import send_report_email
from report_builder import build_report_excel_with_metadata
from report_schema import delete_s3_prefix, write_bytes_to_s3

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
S3_OUTPUT_PREFIX = os.environ.get("S3_OUTPUT_PREFIX", "Output/")


def _output_today_prefix_uri(bucket: str, output_prefix: str, today: str) -> str:
    prefix = output_prefix.strip("/")
    if prefix:
        return f"s3://{bucket}/{prefix}/{today}/"
    return f"s3://{bucket}/{today}/"


@functions_framework.http
def generate_report(request: Request):
    try:
        if not S3_BUCKET_NAME:
            raise RuntimeError("Missing S3_BUCKET_NAME environment variable")

        today = datetime.date.today().strftime("%Y-%m-%d")
        output_today_uri = _output_today_prefix_uri(S3_BUCKET_NAME, S3_OUTPUT_PREFIX, today)

        delete_s3_prefix(output_today_uri)

        outputs = []

        for report_type in (1, 2, 3, 4, 5):
            xlsx_bytes, input_yyyymmdd, roman = build_report_excel_with_metadata(report_type)

            output_filename = f"FT_BC_OC_REPORT_{roman}_{input_yyyymmdd}.xlsx"
            output_uri = f"{output_today_uri}{output_filename}"

            write_bytes_to_s3(
                output_uri,
                xlsx_bytes,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            outputs.append(output_uri)

            # send_report_email(report_type, xlsx_bytes)

        return make_response(("Generated reports:\n" + "\n".join(outputs), 200))

    except Exception as exc:
        return make_response((f"Internal error: {exc}", 500))