import datetime
import os

import functions_framework
from flask import Request, make_response
from report_builder import build_report_excel
from report_schema import delete_s3_prefix, write_bytes_to_s3


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

        xlsx_bytes = build_report_excel(report_type)

        output_root = os.environ.get("OUTPUT_S3_URI_PREFIX")
        output_uri = None
        if output_root:
            if not output_root.startswith("s3://"):
                return make_response(("Invalid OUTPUT_S3_URI_PREFIX", 500))
            if not output_root.endswith("/"):
                output_root = output_root + "/"
            today = datetime.date.today().isoformat()
            date_prefix_uri = output_root + today + "/"
            delete_s3_prefix(date_prefix_uri)
            filename = f"report_tipo_{report_type}.xlsx"
            output_uri = date_prefix_uri + filename
            write_bytes_to_s3(
                output_uri,
                xlsx_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        response = make_response(xlsx_bytes, 200)
        response.headers["Content-Type"] = (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response.headers["Content-Disposition"] = (
            f'attachment; filename="report_tipo_{report_type}.xlsx"'
        )
        if output_uri:
            response.headers["X-Report-S3-Uri"] = output_uri
        return response

    except Exception as e:
        return make_response((f"Internal error: {e}", 500))
