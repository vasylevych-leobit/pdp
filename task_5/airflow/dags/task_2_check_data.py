"""
Task 2: Verify that the partitioned CSV file exists in the
Databricks Volume before triggering the pipeline.
"""

import logging
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

log = logging.getLogger(__name__)

VOLUME_BASE_PATH = "/Volumes/main/default/ecommerce_events"


def partition_path(execution_date: datetime) -> str:
    return (
        f"{VOLUME_BASE_PATH}"
        f"/year={execution_date.year}"
        f"/month={execution_date.month:02d}"
        f"/day={execution_date.day:02d}"
        f"/hour={execution_date.hour:02d}"
        f"/events.csv"
    )


def run(execution_date_str: str, **kwargs) -> bool:
    """
    Returns True if the file exists, raises ValueError otherwise.
    Airflow's PythonSensor will retry if the callable raises or returns False.
    """
    execution_date = datetime.fromisoformat(execution_date_str)
    path = partition_path(execution_date)

    log.info("Checking existence of: %s", path)
    client = WorkspaceClient()

    try:
        info = client.files.get_metadata(path)
        log.info("File found — size: %s bytes", info.content_length)
        return True
    except NotFound:
        log.warning("File not found yet: %s", path)
        return False
