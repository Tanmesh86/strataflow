import json
import sys
from datetime import datetime

import psycopg2
import yaml


from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

CONTRACT_PATH = BASE_DIR / "contracts" / "source_contract.yml"
OUTPUT_PATH = BASE_DIR / "checks" / "dq_report.json"
import os

DB_CONFIG = {
    "host": os.getenv("PULSEFLOW_DB_HOST", "localhost"),
    "port": int(os.getenv("PULSEFLOW_DB_PORT", "5432")),
    "database": os.getenv("PULSEFLOW_DB_NAME", "postgres"),
    "user": os.getenv("PULSEFLOW_DB_USER", "postgres"),
}

if os.getenv("PULSEFLOW_DB_PASSWORD"):
    DB_CONFIG["password"] = os.getenv("PULSEFLOW_DB_PASSWORD")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def check_not_null(cur, table, column):
    cur.execute(
        f'SELECT COUNT(*) FROM public."{table}" WHERE "{column}" IS NULL'
    )
    count = cur.fetchone()[0]

    return {
        "rule": "not_null",
        "table": table,
        "column": column,
        "status": "PASS" if count == 0 else "FAIL",
        "violations": count,
    }


def check_primary_key(cur, table, columns):
    cols = ", ".join(f'"{c}"' for c in columns)

    cur.execute(
        f'''
        SELECT COUNT(*)
        FROM (
            SELECT {cols}
            FROM public."{table}"
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        ) duplicates
        '''
    )

    count = cur.fetchone()[0]

    return {
        "rule": "primary_key_unique",
        "table": table,
        "columns": columns,
        "status": "PASS" if count == 0 else "FAIL",
        "violations": count,
    }


def check_customer_email_unique(cur):
    cur.execute(
        '''
        SELECT COUNT(*)
        FROM (
            SELECT email
            FROM public.customers
            GROUP BY email
            HAVING COUNT(*) > 1
        ) duplicates
        '''
    )

    count = cur.fetchone()[0]

    return {
        "rule": "email_unique",
        "table": "customers",
        "column": "email",
        "status": "PASS" if count == 0 else "FAIL",
        "violations": count,
    }


def check_products(cur):
    checks = []

    cur.execute(
        "SELECT COUNT(*) FROM public.products WHERE price < 0"
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "price_non_negative",
        "table": "products",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    cur.execute(
        "SELECT COUNT(*) FROM public.products WHERE stock < 0"
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "stock_non_negative",
        "table": "products",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    return checks


def check_orders(cur):
    checks = []

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.orders
        WHERE total_amount < 0
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "total_amount_non_negative",
        "table": "orders",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.orders
        WHERE status NOT IN (
            'PENDING',
            'CONFIRMED',
            'SHIPPED',
            'DELIVERED',
            'CANCELLED'
        )
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "valid_order_status",
        "table": "orders",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.orders o
        LEFT JOIN public.customers c
          ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "customer_foreign_key",
        "table": "orders",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    return checks


def check_order_items(cur):
    checks = []

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.order_items
        WHERE quantity <= 0
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "quantity_positive",
        "table": "order_items",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.order_items
        WHERE unit_price < 0
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "unit_price_non_negative",
        "table": "order_items",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.order_items oi
        LEFT JOIN public.orders o
          ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "order_foreign_key",
        "table": "order_items",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.order_items oi
        LEFT JOIN public.products p
          ON oi.product_id = p.product_id
        WHERE p.product_id IS NULL
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "product_foreign_key",
        "table": "order_items",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    return checks


def check_payments(cur):
    checks = []

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.payments
        WHERE amount < 0
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "amount_non_negative",
        "table": "payments",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.payments
        WHERE status NOT IN (
            'PENDING',
            'SUCCESS',
            'FAILED',
            'REFUNDED'
        )
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "valid_payment_status",
        "table": "payments",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.payments p
        LEFT JOIN public.orders o
          ON p.order_id = o.order_id
        WHERE o.order_id IS NULL
        """
    )
    violations = cur.fetchone()[0]

    checks.append({
        "rule": "order_foreign_key",
        "table": "payments",
        "status": "PASS" if violations == 0 else "FAIL",
        "violations": violations,
    })

    return checks


def main():
    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    results = []

    conn = get_connection()

    try:
        cur = conn.cursor()

        for table, definition in contract["tables"].items():

            for column, column_def in definition["columns"].items():
                if column_def.get("nullable") is False:
                    results.append(
                        check_not_null(cur, table, column)
                    )

            results.append(
                check_primary_key(
                    cur,
                    table,
                    definition["primary_key"]
                )
            )

        results.append(check_customer_email_unique(cur))
        results.extend(check_products(cur))
        results.extend(check_orders(cur))
        results.extend(check_order_items(cur))
        results.extend(check_payments(cur))

    finally:
        conn.close()

    failed = sum(
        1 for result in results
        if result["status"] == "FAIL"
    )

    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "PASS" if failed == 0 else "FAIL",
        "total_checks": len(results),
        "passed": len(results) - failed,
        "failed": failed,
        "checks": results,
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("=" * 60)
    print("PULSEFLOW DATA QUALITY REPORT")
    print("=" * 60)

    for result in results:
        print(
            f"[{result['status']}] "
            f"{result['table']} :: "
            f"{result['rule']} :: "
            f"violations={result['violations']}"
        )

    print("=" * 60)
    print(
        f"TOTAL={len(results)} "
        f"PASS={len(results) - failed} "
        f"FAIL={failed}"
    )
    print(f"Report: {OUTPUT_PATH}")
    print("=" * 60)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
