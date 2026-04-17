"""
Task 1: Generate synthetic e-commerce event data using Faker
and upload it to a Databricks Volume partitioned by date/hour.
"""

import csv
import io
import logging
import random
import uuid
from datetime import datetime

from faker import Faker
from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)

VOLUME_BASE_PATH = "/Volumes/main/default/ecommerce_events"

CATEGORY_CODES = [
    "appliances.environment.water_heater",
    "appliances.kitchen.refrigerators",
    "appliances.kitchen.washer",
    "electronics.smartphone",
    "electronics.tablet",
    "electronics.audio.headphone",
    "computers.notebook",
    "computers.desktop",
    "furniture.living_room.sofa",
    "furniture.bedroom.bed",
]

BRANDS = [
    "aqua", "samsung", "apple", "lg", "sony",
    "bosch", "ikea", "philips", "xiaomi", "asus",
]

EVENT_TYPES = ["view", "cart", "purchase"]
EVENT_WEIGHTS = [0.70, 0.20, 0.10]


def generate_rows(execution_date: datetime, num_rows=5000):
    fake = Faker()
    Faker.seed(int(execution_date.timestamp()))
    random.seed(int(execution_date.timestamp()))

    rows = []
    for _ in range(num_rows):
        event_time = fake.date_time_between(
            start_date=execution_date.replace(minute=0, second=0, microsecond=0),
            end_date=execution_date.replace(minute=59, second=59, microsecond=0),
        )
        category_code = random.choice(CATEGORY_CODES)
        category_id = abs(hash(category_code)) % (10**19)

        rows.append({
            "event_time": f"{event_time.strftime('%Y-%m-%d %H:%M:%S')} UTC",
            "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
            "product_id": random.randint(1_000_000, 9_999_999),
            "category_id": category_id,
            "category_code": category_code,
            "brand": random.choice(BRANDS),
            "price": round(random.uniform(5.0, 2000.0), 2),
            "user_id": random.randint(100_000_000, 999_999_999),
            "user_session": str(uuid.uuid4()),
        })
    return rows


def rows_to_csv_bytes(rows):
    fieldnames = [
        "event_time", "event_type", "product_id", "category_id",
        "category_code", "brand", "price", "user_id", "user_session",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def upload_to_volume(
    client: WorkspaceClient,
    execution_date: datetime,
    data: bytes,
) -> str:
    partition_path = (
        f"{VOLUME_BASE_PATH}"
        f"/year={execution_date.year}"
        f"/month={execution_date.month:02d}"
        f"/day={execution_date.day:02d}"
        f"/hour={execution_date.hour:02d}"
    )
    file_path = f"{partition_path}/events.csv"

    log.info("Uploading %d bytes to %s", len(data), file_path)
    client.files.upload(file_path, io.BytesIO(data), overwrite=True)
    log.info("Upload complete: %s", file_path)
    return file_path


def run(execution_date_str: str, num_rows=5000):
    """
    Entry point called by the Airflow PythonOperator.
    execution_date_str: ISO-format string like '2024-01-15T13:00:00+00:00'
    """
    execution_date = datetime.fromisoformat(execution_date_str)
    log.info("Generating %d rows for execution_date=%s", num_rows, execution_date)

    rows = generate_rows(execution_date, num_rows)
    data = rows_to_csv_bytes(rows)

    client = WorkspaceClient()   # reads DATABRICKS_HOST + DATABRICKS_TOKEN env vars
    file_path = upload_to_volume(client, execution_date, data)

    return file_path
