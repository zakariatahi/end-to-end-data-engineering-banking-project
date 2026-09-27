"""Inspect MinIO and load new banking events into Snowflake every 25 minutes."""

from datetime import datetime, timedelta, timezone
from io import BytesIO
import os
from pathlib import Path
import tempfile

import boto3
from airflow.decorators import dag, task
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector


CHUNK_ROWS = 10_000
META_FIELDS = [
    pa.field("_op", pa.string()),
    pa.field("_source_ts_ms", pa.int64()),
    pa.field("_kafka_offset", pa.int64()),
]
SCHEMAS = {
    "CUSTOMERS": pa.schema([
        pa.field("id", pa.int64()),
        pa.field("first_name", pa.string()),
        pa.field("last_name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("created_at", pa.string()),
        *META_FIELDS,
    ]),
    "ACCOUNTS": pa.schema([
        pa.field("id", pa.int64()),
        pa.field("customer_id", pa.int64()),
        pa.field("account_type", pa.string()),
        pa.field("balance", pa.string()),
        pa.field("currency", pa.string()),
        pa.field("created_at", pa.string()),
        *META_FIELDS,
    ]),
    "TRANSACTIONS": pa.schema([
        pa.field("id", pa.int64()),
        pa.field("account_id", pa.int64()),
        pa.field("txn_type", pa.string()),
        pa.field("amount", pa.string()),
        pa.field("related_account_id", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("created_at", pa.string()),
        *META_FIELDS,
    ]),
}


@dag(
    dag_id="inspect_banking_minio",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(minutes=25),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["banking"],
)
def inspect_banking_minio():
    @task
    def count_parquet_files():
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        )
        bucket = os.environ["MINIO_BUCKET"]
        paginator = s3.get_paginator("list_objects_v2")
        for table in ("customers", "accounts", "transactions"):
            pages = paginator.paginate(Bucket=bucket, Prefix=f"{table}/")
            count = sum(
                1
                for page in pages
                for obj in page.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            )
            print(f"{table}: {count} Parquet files in MinIO")

    @task
    def load_table(table_name: str) -> None:
        if table_name not in SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name}")

        connection = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database="BANKING",
            schema="RAW",
        )
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        )
        bucket = os.environ["MINIO_BUCKET"]
        buffer = []
        seen_offsets = set()
        loaded_rows = 0
        scanned_files = 0

        with connection, connection.cursor() as cursor:
            cursor.execute(f"SELECT MAX(v:_kafka_offset::NUMBER) FROM {table_name}")
            last_loaded = cursor.fetchone()[0]
            next_offset = int(last_loaded) + 1 if last_loaded is not None else 0
            print(f"{table_name}: resuming at Kafka offset {next_offset}")

            def upload_chunk(rows: list[dict]) -> None:
                nonlocal loaded_rows
                start = rows[0]["_kafka_offset"]
                end = rows[-1]["_kafka_offset"]
                filename = f"{table_name.lower()}_{start:012d}-{end:012d}.parquet"
                with tempfile.TemporaryDirectory() as directory:
                    local_file = Path(directory) / filename
                    pq.write_table(
                        pa.Table.from_pylist(rows, schema=SCHEMAS[table_name]),
                        local_file,
                    )
                    cursor.execute(
                        f"PUT file://{local_file} @%{table_name} "
                        "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
                    )
                    cursor.execute(
                        f"COPY INTO {table_name} FROM @%{table_name} "
                        "FILE_FORMAT=(TYPE=PARQUET) "
                        f"FILES=('{filename}') ON_ERROR=ABORT_STATEMENT"
                    )
                    result = cursor.fetchall()
                    copied = sum(int(item[3]) for item in result)
                    if copied != len(rows):
                        raise RuntimeError(
                            f"Expected {len(rows)} rows from {filename}, copied {copied}: {result}"
                        )
                    loaded_rows += copied
                    print(f"{table_name}: loaded offsets {start}-{end} ({copied} rows)")

            pages = s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=f"{table_name.lower()}/"
            )
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(".parquet"):
                        continue
                    if "/partition=0/" not in key:
                        raise ValueError(f"Unexpected Kafka partition in {key}")
                    scanned_files += 1
                    payload = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                    for row in pq.read_table(BytesIO(payload)).to_pylist():
                        offset = int(row["_kafka_offset"])
                        if offset < next_offset or offset in seen_offsets:
                            continue
                        if offset != next_offset:
                            raise ValueError(
                                f"{table_name}: missing offset {next_offset} before {offset}"
                            )
                        seen_offsets.add(offset)
                        buffer.append(row)
                        next_offset += 1
                        if len(buffer) == CHUNK_ROWS:
                            upload_chunk(buffer)
                            buffer.clear()
            if buffer:
                upload_chunk(buffer)
            print(
                f"{table_name}: scanned {scanned_files} files; "
                f"loaded {loaded_rows} new rows"
            )

    loads = {
        table: load_table.override(task_id=f"load_{table.lower()}")(table)
        for table in SCHEMAS
    }
    # Load children before parents while the generator continues inserting rows.
    count_parquet_files() >> loads["TRANSACTIONS"] >> loads["ACCOUNTS"] >> loads["CUSTOMERS"]


inspect_banking_minio()
