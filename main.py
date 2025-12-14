import datetime
import os

import functions_framework
from flask import Request, make_response

from report_builder import build_reports_excels_with_metadata
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
            return make_response(("Missing S3_BUCKET_NAME environment variable", 500))

        today = datetime.date.today().strftime("%Y-%m-%d")
        output_today_uri = _output_today_prefix_uri(S3_BUCKET_NAME, S3_OUTPUT_PREFIX, today)

        delete_s3_prefix(output_today_uri)

        written = []
        skipped = []
        failed = []

        for report_type in (1, 2, 3, 4, 5):
            try:
                outputs = build_reports_excels_with_metadata(report_type)

                for xlsx_bytes, input_yyyymmdd, roman, suffix in outputs:
                    if report_type == 1:
                        output_filename = f"FT_BC_OC_REPORT_{roman}_{input_yyyymmdd}_{suffix}.xlsx"
                    else:
                        output_filename = f"FT_BC_OC_REPORT_{roman}_{input_yyyymmdd}.xlsx"

                    output_uri = f"{output_today_uri}{output_filename}"

                    write_bytes_to_s3(
                        output_uri,
                        xlsx_bytes,
                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

                    written.append(output_uri)

            except Exception as exc:
                msg = str(exc)
                if "No matching CSV found for report_type" in msg:
                    skipped.append(f"type {report_type}: {msg}")
                else:
                    failed.append(f"type {report_type}: {repr(exc)}")

        lines = []
        lines.append(f"output_prefix: {output_today_uri}")
        lines.append(f"written: {len(written)}")
        lines.extend(written)
        lines.append(f"skipped: {len(skipped)}")
        lines.extend(skipped)
        lines.append(f"failed: {len(failed)}")
        lines.extend(failed)

        status = 200 if len(written) > 0 and len(failed) == 0 else 500 if len(written) == 0 else 207
        return make_response(("\n".join(lines), status))

    except Exception as exc:
        return make_response((f"Internal error: {exc}", 500))
