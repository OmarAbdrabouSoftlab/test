import io
import pandas as pd

from report_schema import get_latest_csv_uri, read_bytes_from_s3
from typing import Any, Dict


def load_source_dataframe(config: Dict[str, Any], source_name: str) -> pd.DataFrame:
    sources = config["sources"]
    if source_name not in sources:
        raise KeyError(f"Source '{source_name}' not defined in config['sources']")
    src_conf = sources[source_name]
    prefix_uri = src_conf["path"]
    delimiter = src_conf.get("delimiter", ",")
    encoding = src_conf.get("encoding", "utf-8")
    header_flag = src_conf.get("header", True)
    decimal_sep = src_conf.get("decimal", ".")
    thousands_sep = src_conf.get("thousands", None)
    latest_uri = get_latest_csv_uri(prefix_uri)
    data_bytes = read_bytes_from_s3(latest_uri)
    df = pd.read_csv(
        io.BytesIO(data_bytes),
        sep=delimiter,
        encoding=encoding,
        header=0 if header_flag else None,
        decimal=decimal_sep,
        thousands=thousands_sep
    )
    return df
