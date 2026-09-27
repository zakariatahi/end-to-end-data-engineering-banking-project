"""Save PostgreSQL change events from Kafka as Parquet objects in MinIO."""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
import json
import os
from pathlib import Path
import time

import boto3
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata
import pyarrow as pa
import pyarrow.parquet as pq


TOPICS = (
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
)
BATCH_SIZE = 50
FLUSH_SECONDS = 5


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-batches", type=int, help="Stop after this many uploads")
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "bankingminio")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "bankingminio123")
    bucket = os.environ.get("MINIO_BUCKET", "banking-cdc")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    if bucket not in {item["Name"] for item in s3.list_buckets()["Buckets"]}:
        s3.create_bucket(Bucket=bucket)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=bootstrap,
        group_id="banking-minio-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    pending = defaultdict(list)
    uploaded = 0
    last_flush = time.monotonic()
    print(f"Listening to {len(TOPICS)} Kafka topics; uploading to {bucket}.", flush=True)

    def upload(topic_partition, count=None) -> None:
        nonlocal uploaded
        messages = pending[topic_partition]
        if not messages:
            return
        batch = messages[:count]
        rows = []
        for message in batch:
            event = json.loads(message.value) if message.value else None
            if event is None:
                continue
            row = dict(event.get("after") or event.get("before") or {})
            row["_op"] = event.get("op")
            row["_source_ts_ms"] = event.get("source", {}).get("ts_ms")
            row["_kafka_offset"] = message.offset
            rows.append(row)

        if rows:
            output = BytesIO()
            pq.write_table(pa.Table.from_pylist(rows), output)
            table = topic_partition.topic.rsplit(".", 1)[-1]
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            key = (
                f"{table}/date={date}/partition={topic_partition.partition}/"
                f"{batch[0].offset:012d}-{batch[-1].offset:012d}.parquet"
            )
            s3.put_object(Bucket=bucket, Key=key, Body=output.getvalue())
            print(f"Uploaded {len(rows)} {table} events to {key}", flush=True)
            uploaded += 1

        # Only acknowledge Kafka after MinIO has accepted the whole batch.
        consumer.commit(
            {topic_partition: OffsetAndMetadata(batch[-1].offset + 1, "")}
        )
        del messages[: len(batch)]

    try:
        while True:
            for topic_partition, messages in consumer.poll(
                timeout_ms=1000, max_records=BATCH_SIZE
            ).items():
                pending[topic_partition].extend(messages)
                while len(pending[topic_partition]) >= BATCH_SIZE:
                    upload(topic_partition, BATCH_SIZE)
                    if args.max_batches and uploaded >= args.max_batches:
                        return
            if time.monotonic() - last_flush >= FLUSH_SECONDS:
                for topic_partition in list(pending):
                    upload(topic_partition)
                    if args.max_batches and uploaded >= args.max_batches:
                        return
                last_flush = time.monotonic()
    except KeyboardInterrupt:
        print("Stopping after uploading pending events.", flush=True)
    finally:
        for topic_partition in list(pending):
            upload(topic_partition)
        consumer.close()


if __name__ == "__main__":
    main()
