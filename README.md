# Banking data pipeline

We are building this project step by step, using [banking-modern-datastack](https://github.com/Jay61616/banking-modern-datastack) as a reference.

## Python environment

The generator uses Faker for names, psycopg2 to write to PostgreSQL, and python-dotenv to read local connection settings. Dependencies are recorded in `pyproject.toml` and `uv.lock`. Run `uv sync` to install them, then use `uv run` for the script.

## Step 1: PostgreSQL source database

The source database models three things:

- A **customer** is a person. `customers.id` identifies one customer.
- An **account** belongs to a customer through `accounts.customer_id`.
- A **transaction** belongs to an account through `transactions.account_id`. For transfers, `related_account_id` points to the destination account.

`NUMERIC(18, 2)` stores money as exact decimal values. Primary keys identify rows; foreign keys keep relationships valid. The checks reject negative balances and nonpositive transaction amounts.

Start the database with Docker Desktop running:

```powershell
docker compose up -d postgres
docker compose ps
```

Inspect the tables and sample data:

```powershell
docker compose exec postgres psql -U banking -d banking -c "\dt"
docker compose exec postgres psql -U banking -d banking -c "SELECT c.first_name, a.id AS account_id, a.balance FROM customers c JOIN accounts a ON a.customer_id = c.id ORDER BY a.id;"
```

The SQL files under `postgres/` run when Docker creates the database volume for the first time. Editing them later does not change an existing database; we will use migrations for later schema changes. This project exposes PostgreSQL on host port `5434` because another local service uses `5432`. The password in `compose.yaml` is for a local learning environment only. You can override it with `POSTGRES_PASSWORD` in a local `.env` file, which Git ignores.

## Step 2: Generate synthetic data

Copy the local database settings once, then run one batch:

```powershell
Copy-Item .env.example .env
uv run python data-generator/generate.py --once
```

Run without `--once` to keep adding a batch every 2 seconds until you press Ctrl+C:

```powershell
uv run python data-generator/generate.py
```

Like the reference project, each batch inserts 10 new customers, two accounts per customer, and 50 activity transactions **directly into PostgreSQL**. Faker supplies first and last names. Each email combines those names with a random number at the end, such as `zakariatahiri89@gmail.com`. The number comes from 0–99 in 50% of attempts, 0–999 in 25%, and 0–9999 in 25%. If an email already exists, the generator tries another number. We also record 20 opening deposits and update balances for deposits, withdrawals, and transfers, so each batch adds 70 transaction rows in total. A batch commits together, or rolls back together if something fails.

You can adjust the batch with `--customers`, `--transactions`, and `--interval`. Use `--seed 42` for repeatable random choices in a demonstration.

## Step 3: Kafka broker without ZooKeeper

This project uses a single Apache Kafka broker in KRaft mode. Start it with:

```powershell
docker compose up -d kafka
docker compose ps
```

Tools on your computer connect to `localhost:9092`; other Docker services connect to `kafka:19092`.

## Step 4: Stream PostgreSQL changes with Debezium

PostgreSQL starts with `wal_level=logical` so Debezium can read row changes from its write-ahead log. Start Kafka Connect with the Debezium connector:

```powershell
docker compose up -d connect
```

Register the connector once from PowerShell:

```powershell
Invoke-RestMethod -Uri http://localhost:8083/connectors -Method Post -ContentType application/json -InFile kafka-debezium/postgres-connector.json
```

Check its status:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/banking-postgres/status | ConvertTo-Json -Depth 5
```

Read one new customer event (the command waits until the next generated customer arrives):

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic banking_server.public.customers --max-messages 1 --timeout-ms 20000
```

On first start, Debezium copies the current rows (`"op":"r"`), then follows new inserts (`"op":"c"`) and updates (`"op":"u"`). It publishes to `banking_server.public.customers`, `banking_server.public.accounts`, and `banking_server.public.transactions`. The connector settings are in `kafka-debezium/postgres-connector.json`. Kafka Connect reads the database password from its environment, so the JSON file does not contain the password. `decimal.handling.mode=string` keeps money values exact in JSON.

## Step 5: Save Kafka events to MinIO

Start MinIO and install the Python dependencies:

```powershell
docker compose up -d minio
uv sync
```

Run the consumer continuously:

```powershell
uv run python consumer/kafka_to_minio.py
```

The consumer reads all three banking Kafka topics. It writes batches of up to 50 events as Parquet objects under `customers/`, `accounts/`, and `transactions/` in the `banking-cdc` bucket. It saves the Kafka offset only after MinIO accepts the batch. Each row also includes `_op` (`r`, `c`, `u`, or `d`), `_source_ts_ms`, and `_kafka_offset`, so the change history remains understandable. The values of money fields stay as exact strings.

Open the MinIO console at <http://localhost:9001> and sign in using `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` from `.env`. The Compose setup uses a [community MinIO build](https://github.com/golithus/minio-builds) because the original image registries were unavailable when this step was built.

## Step 6: Start learning Airflow

Start the local Airflow instance:

```powershell
docker compose up -d airflow
```

Open <http://localhost:8080>. The username is `admin`; get the generated password with:

```powershell
docker compose exec -T airflow cat /opt/airflow/standalone_admin_password.txt
```

Airflow runs a **DAG**, a group of tasks with a schedule or a manual trigger. The `inspect_banking_minio` DAG counts Parquet files under the three table folders in MinIO, then loads new transactions, accounts, and customers into Snowflake. Inspection and loading are defined together in `airflow/dags/inspect_minio.py` and run every 25 minutes. Find it in the Airflow UI to inspect task logs or trigger an extra run. You can also run it from the terminal:

```powershell
docker compose exec -T airflow airflow dags test inspect_banking_minio 2026-09-27
```

This learning setup uses Airflow's single-container standalone mode and its own SQLite metadata database. The reference project uses separate Airflow services and a PostgreSQL metadata database; we can add that when we need a larger workflow.

## Step 7: Configure Snowflake loading

The combined `inspect_banking_minio` DAG uploads batches to Snowflake's table stages with `PUT`, then loads them into `BANKING.RAW` with `COPY INTO`. Each Parquet row becomes one object in the table's `v VARIANT` column. The earlier one-file demo DAG has been removed.

Add your Snowflake account identifier, username, password, and warehouse name to the ignored local `.env` file using the `SNOWFLAKE_*` names in `.env.example`. Then refresh the Airflow container's environment:

```powershell
docker compose up -d airflow
```

Trigger `inspect_banking_minio` in Airflow. Query the result in Snowflake:

```sql
SELECT COUNT(*) FROM BANKING.RAW.CUSTOMERS;
SELECT v:id, v:first_name, v:email, v:_op FROM BANKING.RAW.CUSTOMERS LIMIT 5;
```

The loader resumes from the largest Kafka offset already in each Snowflake raw table, so subsequent runs load new events.

## Step 8: Backfill all three Snowflake raw tables

The combined `inspect_banking_minio` DAG also backfills all available events. Trigger it in Airflow, or run it from PowerShell:

```powershell
docker compose exec -T airflow airflow dags test inspect_banking_minio 2026-09-27
```

After counting files, its three loading tasks run in order: `TRANSACTIONS`, `ACCOUNTS`, then `CUSTOMERS`. Each task reads its table's Parquet files from MinIO, combines rows into batches of up to 10,000, uploads each batch to the Snowflake table stage, and runs `COPY INTO` the matching `BANKING.RAW` table. The raw tables each have one `v VARIANT` column containing the source fields and Kafka metadata. The tasks resume after the largest `_kafka_offset` already in Snowflake and check that no Kafka offsets are missing.

Check the loaded row counts in a Snowflake worksheet:

```sql
SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS rows_loaded FROM BANKING.RAW.CUSTOMERS
UNION ALL
SELECT 'ACCOUNTS', COUNT(*) FROM BANKING.RAW.ACCOUNTS
UNION ALL
SELECT 'TRANSACTIONS', COUNT(*) FROM BANKING.RAW.TRANSACTIONS;
```

This DAG runs on a 25-minute interval using `schedule=timedelta(minutes=25)`, with no backfill of missed scheduled runs and at most one active run. Snapshots and marts have a separate hourly schedule (Step 14).

## Step 9: Initialize dbt

The `banking_dbt` project is configured to connect to Snowflake using the `SNOWFLAKE_*` values in the ignored root `.env`. Its development models will go in the `BANKING.ANALYTICS` schema, keeping them separate from `BANKING.RAW`.

From the project root, check the dbt configuration and Snowflake connection:

```powershell
uv run --env-file .env dbt debug --project-dir banking_dbt --profiles-dir banking_dbt
```

The expected result is `All checks passed!`.

## Step 10: Build the first customer model

`models/sources.yml` names the raw Snowflake customer table. `models/staging/stg_customers.sql` reads its `v` object, converts the customer fields into typed columns, and keeps the latest Kafka event for each customer ID. If that latest event is a deletion, the customer is excluded.

Run just this model:

```powershell
uv run --env-file .env dbt run --project-dir banking_dbt --profiles-dir banking_dbt --select stg_customers
```

Inspect the resulting view in Snowflake:

```sql
SELECT customer_id, first_name, last_name, email, created_at
FROM BANKING.ANALYTICS.STG_CUSTOMERS
ORDER BY customer_id
LIMIT 5;
```

## Step 11: Build account and transaction models

`stg_accounts` and `stg_transactions` read their matching raw tables and expose typed columns. They keep the latest Kafka event for each account or transaction ID and exclude deleted records. Balances and amounts use `NUMBER(18,2)` so cents stay exact.

```powershell
uv run --env-file .env dbt run --project-dir banking_dbt --profiles-dir banking_dbt --select stg_accounts stg_transactions
```

Inspect the new views in a Snowflake worksheet:

```sql
SELECT * FROM BANKING.ANALYTICS.STG_ACCOUNTS LIMIT 5;
SELECT * FROM BANKING.ANALYTICS.STG_TRANSACTIONS LIMIT 5;
```

## Step 12: Record customer and account snapshots

The dbt snapshots in `banking_dbt/snapshots/` track changes in customer names or email and account ownership, type, balance, or currency. They read the current staging views and add `DBT_VALID_FROM` and `DBT_VALID_TO` to each version. A `NULL` `DBT_VALID_TO` marks the current version. Deleted source rows close their current version.

```powershell
uv run --env-file .env dbt snapshot --project-dir banking_dbt --profiles-dir banking_dbt --select customers_snapshot accounts_snapshot
```

Inspect account history in Snowflake:

```sql
SELECT account_id, balance, dbt_valid_from, dbt_valid_to
FROM BANKING.ANALYTICS.ACCOUNTS_SNAPSHOT
WHERE account_id = 1
ORDER BY dbt_valid_from;
```

The first run records the current state. Run the command again after new changes have reached Snowflake to record later versions. The raw CDC tables already contain earlier events, but dbt snapshots do not reconstruct those events retroactively.

## Step 13: Build the banking marts

`dim_customers` and `dim_accounts` are tables built from the snapshots. Each row represents one historical version; `effective_from`, `effective_to`, and `is_current` describe its validity. When joining by customer or account ID for current-state reporting, filter the dimension to `is_current = TRUE` to avoid counting multiple versions.

`fact_transactions` has one row per transaction ID. It reads `stg_transactions` and gets the customer ID from the current account staging view. The first run loads every transaction; later runs merge only transaction events with a newer Kafka offset. Run snapshots before refreshing the dimension tables:

```powershell
uv run --env-file .env dbt snapshot --project-dir banking_dbt --profiles-dir banking_dbt
uv run --env-file .env dbt run --project-dir banking_dbt --profiles-dir banking_dbt --select dim_customers dim_accounts fact_transactions
```

Example report in Snowflake:

```sql
SELECT a.currency, f.transaction_type,
       COUNT(*) AS transaction_count,
       SUM(f.amount) AS transaction_volume
FROM BANKING.ANALYTICS.FACT_TRANSACTIONS AS f
JOIN BANKING.ANALYTICS.DIM_ACCOUNTS AS a
  ON f.account_id = a.account_id AND a.is_current
GROUP BY a.currency, f.transaction_type
ORDER BY a.currency, f.transaction_type;
```

The incremental fact supports new transactions and transaction updates. If transactions are deleted or an existing account changes owner, rebuild it with `--full-refresh` so previously loaded fact rows reflect the current staging data. Historical owner attribution would require a different join using the snapshot validity periods.

## Step 14: Refresh snapshots and marts hourly with Airflow

The `dbt_snapshots_marts_hourly` DAG runs at the start of each UTC hour. `run_snapshots` records customer/account history, then `run_marts` refreshes both dimensions and merges new transaction events into the fact table. If snapshots fail, marts do not run. Missed hours are not backfilled, and only one workflow run can be active at a time. This DAG uses the raw data already loaded by the separate 25-minute `inspect_banking_minio` DAG.

The Airflow image retains the dependencies used by the ingestion DAGs. The dbt project is mounted read-only, and dbt runs in a cached isolated environment under the Airflow volume. Snowflake credentials come from the container environment configured in Compose. Logs and compiled dbt files go under `/opt/airflow/dbt_runs/`.

Deploy and enable the DAG:

```powershell
docker compose up -d --build airflow
docker compose exec -T airflow airflow dags unpause inspect_banking_minio
docker compose exec -T airflow airflow dags unpause dbt_snapshots_marts_hourly
```

Open <http://localhost:8080> and select `dbt_snapshots_marts_hourly` to inspect task logs or trigger an extra run. Docker and Airflow must remain running for the hourly schedule.

The generator and Kafka-to-MinIO consumer run continuously as separate Python processes (see Steps 2 and 5). PostgreSQL, Kafka, and MinIO receive new data continuously; Snowflake raw tables refresh every 25 minutes, and snapshots and marts refresh hourly. You can also trigger either DAG manually. Background process logs from this session are in `data/logs/pipeline-generator.stdout.log` and `data/logs/pipeline-consumer.stdout.log`, with matching `.stderr.log` files for errors.
