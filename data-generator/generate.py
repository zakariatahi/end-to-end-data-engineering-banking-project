"""Continuously generate synthetic customers, accounts, and transactions in PostgreSQL."""

import argparse
import os
import random
import re
import time
import unicodedata
from decimal import Decimal
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from faker import Faker


ACCOUNT_TYPES = ("CHECKING", "SAVINGS")
CURRENCY = "USD"
MIN_INITIAL_CENTS = 1_000
MAX_INITIAL_CENTS = 100_000
MAX_TRANSACTION_CENTS = 100_000


def money(cents: int) -> Decimal:
    """Convert integer cents to an exact database money value."""
    return Decimal(cents) / 100


def email_name_part(name: str) -> str:
    """Turn a name into letters and digits suitable for an email address."""
    plain = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", plain.lower()) or "customer"


def customer_email(first_name: str, last_name: str, rng: random.Random) -> str:
    """Use a 0-99 suffix 50% of the time, 0-999 25%, or 0-9999 25%."""
    roll = rng.randrange(100)
    upper = 99 if roll < 50 else 999 if roll < 75 else 9999
    number = rng.randint(0, upper)
    return f"{email_name_part(first_name)}{email_name_part(last_name)}{number}@gmail.com"


def run_batch(connection, fake: Faker, rng: random.Random, customer_count: int, transaction_count: int) -> None:
    """Create one complete batch; roll it all back if any insert fails."""
    balances = {}

    with connection:
        with connection.cursor() as cursor:
            for _ in range(customer_count):
                first_name = fake.first_name()
                last_name = fake.last_name()
                for _ in range(100):
                    email = customer_email(first_name, last_name, rng)
                    cursor.execute(
                        "INSERT INTO customers (first_name, last_name, email) "
                        "VALUES (%s, %s, %s) ON CONFLICT (email) DO NOTHING RETURNING id",
                        (first_name, last_name, email),
                    )
                    inserted = cursor.fetchone()
                    if inserted:
                        customer_id = inserted[0]
                        break
                else:
                    raise RuntimeError(f"Could not find an unused email for {first_name} {last_name}")

                for account_type in ACCOUNT_TYPES:
                    initial_cents = rng.randint(MIN_INITIAL_CENTS, MAX_INITIAL_CENTS)
                    cursor.execute(
                        "INSERT INTO accounts (customer_id, account_type, balance, currency) "
                        "VALUES (%s, %s, %s, %s) RETURNING id",
                        (customer_id, account_type, money(initial_cents), CURRENCY),
                    )
                    account_id = cursor.fetchone()[0]
                    balances[account_id] = initial_cents
                    # Record the opening balance so the transaction history explains it.
                    cursor.execute(
                        "INSERT INTO transactions (account_id, txn_type, amount) "
                        "VALUES (%s, 'DEPOSIT', %s)",
                        (account_id, money(initial_cents)),
                    )

            account_ids = list(balances)
            for _ in range(transaction_count):
                source_id = rng.choice(account_ids)
                txn_type = rng.choice(("DEPOSIT", "WITHDRAWAL", "TRANSFER"))
                if txn_type != "DEPOSIT" and balances[source_id] < 100:
                    txn_type = "DEPOSIT"

                limit = MAX_TRANSACTION_CENTS
                if txn_type != "DEPOSIT":
                    limit = min(limit, balances[source_id])
                amount_cents = rng.randint(100, limit)
                amount = money(amount_cents)
                destination_id = None

                if txn_type == "DEPOSIT":
                    balances[source_id] += amount_cents
                    cursor.execute(
                        "UPDATE accounts SET balance = balance + %s WHERE id = %s",
                        (amount, source_id),
                    )
                else:
                    balances[source_id] -= amount_cents
                    cursor.execute(
                        "UPDATE accounts SET balance = balance - %s WHERE id = %s",
                        (amount, source_id),
                    )
                    if txn_type == "TRANSFER":
                        destination_id = rng.choice([id_ for id_ in account_ids if id_ != source_id])
                        balances[destination_id] += amount_cents
                        cursor.execute(
                            "UPDATE accounts SET balance = balance + %s WHERE id = %s",
                            (amount, destination_id),
                        )

                cursor.execute(
                    "INSERT INTO transactions "
                    "(account_id, txn_type, amount, related_account_id, status) "
                    "VALUES (%s, %s, %s, %s, 'COMPLETED')",
                    (source_id, txn_type, amount, destination_id),
                )

    opening_count = customer_count * len(ACCOUNT_TYPES)
    print(
        f"Added {customer_count} customers, {opening_count} accounts, "
        f"and {opening_count + transaction_count} transactions "
        f"({opening_count} opening deposits + {transaction_count} activity).",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one batch, then stop")
    parser.add_argument("--customers", type=int, default=10, help="New customers per batch (default: 10)")
    parser.add_argument("--transactions", type=int, default=50, help="Activity transactions per batch (default: 50)")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between batches (default: 2)")
    parser.add_argument("--seed", type=int, help="Optional seed for repeatable random choices")
    args = parser.parse_args()
    if args.customers < 1 or args.transactions < 0 or args.interval <= 0:
        parser.error("--customers must be positive, --transactions nonnegative, and --interval positive")

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        parser.error("POSTGRES_PASSWORD is missing; copy .env.example to .env")

    rng = random.Random(args.seed)
    fake = Faker()
    if args.seed is not None:
        fake.seed_instance(args.seed)

    try:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5434"),
            dbname=os.getenv("POSTGRES_DB", "banking"),
            user=os.getenv("POSTGRES_USER", "banking"),
            password=password,
            connect_timeout=5,
        )
    except psycopg2.OperationalError as exc:
        parser.exit(1, f"Could not connect to banking PostgreSQL: {exc}\nStart Docker Desktop and run 'docker compose up -d postgres'.\n")
    try:
        batch_number = 0
        while True:
            batch_number += 1
            print(f"Starting batch {batch_number}...", flush=True)
            run_batch(connection, fake, rng, args.customers, args.transactions)
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopped generating data.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
