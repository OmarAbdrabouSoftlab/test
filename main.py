import datetime
import os
import traceback

import functions_framework
from flask import Request, make_response

from report_builder import build_report_excel_with_metadata
from report_schema import delete_s3_prefix, load_config, parse_s3_uri, _as_s3_prefix_uri, get_latest_json_uri

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
S3_CONFIG_PREFIX = os.environ.get("S3_CONFIG_PREFIX", "Config/")
S3_OUTPUT_PREFIX = os.environ.get("S3_OUTPUT_PREFIX", "Output/")


def _output_today_prefix_uri(bucket: str, output_prefix: str, today: str) -> str:
    prefix = output_prefix.strip("/")
    if prefix:
        return f"s3://{bucket}/{prefix}/{today}/"
    return f"s3://{bucket}/{today}/"


def _fmt_debug(debug: dict) -> str:
    lines = []
    for k, v in debug.items():
        lines.append(f"{k}: {v}")
    return "\n".join(lines) + "\n"


@functions_framework.http
def generate_report(request: Request):
    debug_mode = request.args.get("debug") == "1"
    debug = {}

    try:
        debug["env.S3_BUCKET_NAME"] = S3_BUCKET_NAME
        debug["env.S3_CONFIG_PREFIX"] = S3_CONFIG_PREFIX
        debug["env.S3_OUTPUT_PREFIX"] = S3_OUTPUT_PREFIX

        if not S3_BUCKET_NAME:
            raise RuntimeError("Missing S3_BUCKET_NAME environment variable")

        config_prefix_uri = _as_s3_prefix_uri(S3_BUCKET_NAME, S3_CONFIG_PREFIX)
        debug["config.prefix_uri"] = config_prefix_uri

        # Identify which config JSON is chosen (since you load "latest JSON under prefix")
        try:
            config_uri = get_latest_json_uri(config_prefix_uri)
            debug["config.selected_json_uri"] = config_uri
        except Exception as e:
            debug["config.selected_json_uri_error"] = repr(e)
            raise

        config = load_config()
        debug["config.version"] = config.get("version")
        debug["config.sources.main_csv.path"] = config.get("sources", {}).get("main_csv", {}).get("path")

        # If you just want to debug S3 paths, you can stop here.
        if debug_mode and request.args.get("phase") == "config":
            return make_response((_fmt_debug(debug), 200))

        today = datetime.date.today().strftime("%Y-%m-%d")
        output_today_uri = _output_today_prefix_uri(S3_BUCKET_NAME, S3_OUTPUT_PREFIX, today)
        debug["output.today_prefix_uri"] = output_today_uri

        # Try listing/deleting output prefix (this will validate bucket existence for output side)
        delete_s3_prefix(output_today_uri)
        debug["output.delete_prefix"] = "ok"

        outputs = []

        for report_type in (1, 2, 3, 4, 5):
            debug[f"report.{report_type}.start"] = "ok"
            xlsx_bytes, input_yyyymmdd, roman = build_report_excel_with_metadata(report_type)

            output_filename = f"FT_BC_OC_REPORT_{roman}_{input_yyyymmdd}.xlsx"
            output_uri = f"{output_today_uri}{output_filename}"
            outputs.append(output_uri)

        debug["result.outputs_count"] = len(outputs)
        debug["result.outputs"] = outputs

        if debug_mode:
            return make_response((_fmt_debug(debug), 200))

        return make_response(("OK", 200))

    except Exception as exc:
        debug["error"] = repr(exc)
        debug["traceback"] = traceback.format_exc()

        if debug_mode:
            return make_response((_fmt_debug(debug), 500))

        return make_response((f"Internal error: {exc}", 500))