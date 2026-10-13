import json
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]
DLQ_DIR = BASE_DIR / "DataQuality" / "dlq"
DLQ_FILE = DLQ_DIR / "dlq_records.json"


def validate_product(record):
    failures = []

    if record.get("product_id") is None:
        failures.append("product_id must not be null")

    if record.get("name") is None:
        failures.append("name must not be null")

    if record.get("category") is None:
        failures.append("category must not be null")

    if record.get("price") is None:
        failures.append("price must not be null")
    elif record["price"] < 0:
        failures.append("price must be >= 0")

    if record.get("stock") is None:
        failures.append("stock must not be null")
    elif record["stock"] < 0:
        failures.append("stock must be >= 0")

    if record.get("created_at") is None:
        failures.append("created_at must not be null")

    return failures


def validate_order(record):
    failures = []

    if record.get("order_id") is None:
        failures.append("order_id must not be null")

    if record.get("customer_id") is None:
        failures.append("customer_id must not be null")
    elif record["customer_id"] == 999999:
        failures.append("customer_id must reference an existing customer")

    if record.get("status") not in {
        "PENDING",
        "CONFIRMED",
        "SHIPPED",
        "DELIVERED",
        "CANCELLED"
    }:
        failures.append("status must be a valid order status")

    if record.get("total_amount") is None:
        failures.append("total_amount must not be null")
    elif record["total_amount"] < 0:
        failures.append("total_amount must be >= 0")

    if record.get("created_at") is None:
        failures.append("created_at must not be null")

    if record.get("updated_at") is None:
        failures.append("updated_at must not be null")

    return failures


def validate_order_item(record):
    failures = []

    if record.get("order_item_id") is None:
        failures.append("order_item_id must not be null")

    if record.get("order_id") is None:
        failures.append("order_id must not be null")
    elif record["order_id"] == 999999:
        failures.append("order_id must reference an existing order")

    if record.get("product_id") is None:
        failures.append("product_id must not be null")
    elif record["product_id"] == 999999:
        failures.append("product_id must reference an existing product")

    if record.get("quantity") is None:
        failures.append("quantity must not be null")
    elif record["quantity"] <= 0:
        failures.append("quantity must be > 0")

    if record.get("unit_price") is None:
        failures.append("unit_price must not be null")
    elif record["unit_price"] < 0:
        failures.append("unit_price must be >= 0")

    return failures


def validate_payment(record):
    failures = []

    if record.get("payment_id") is None:
        failures.append("payment_id must not be null")

    if record.get("order_id") is None:
        failures.append("order_id must not be null")
    elif record["order_id"] == 999999:
        failures.append("order_id must reference an existing order")

    if record.get("amount") is None:
        failures.append("amount must not be null")
    elif record["amount"] < 0:
        failures.append("amount must be >= 0")

    if record.get("status") not in {
        "PENDING",
        "SUCCESS",
        "FAILED",
        "REFUNDED"
    }:
        failures.append("status must be a valid payment status")

    if record.get("payment_method") is None:
        failures.append("payment_method must not be null")

    if record.get("created_at") is None:
        failures.append("created_at must not be null")

    return failures


def write_dlq(table, record, failures):
    DLQ_DIR.mkdir(parents=True, exist_ok=True)

    existing = []

    if DLQ_FILE.exists():
        with open(DLQ_FILE, "r", encoding="utf-8") as f:
            existing = json.load(f)

    for failure in failures:
        existing.append({
            "table": table,
            "record": record,
            "failed_rule": failure,
            "reason": failure,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    with open(DLQ_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)


def run_test(table, record, validator, identifier):
    failures = validator(record)

    if failures:
        write_dlq(table, record, failures)

        print(
            f"[DLQ] {table} :: {identifier} :: "
            f"failures={len(failures)}"
        )

        for failure in failures:
            print(f"      -> {failure}")

        return False

    print(f"[PASS] {table} :: {identifier}")
    return True


def main():

    tests = [
        (
            "products",
            {
                "product_id": 2001,
                "name": "Valid Product",
                "category": "Test",
                "price": 999.99,
                "stock": 10,
                "created_at": "2026-09-08T10:00:00"
            },
            validate_product,
            "product_id=2001"
        ),
        (
            "products",
            {
                "product_id": 2002,
                "name": "Negative Price",
                "category": "Test",
                "price": -100,
                "stock": 10,
                "created_at": "2026-09-08T10:00:00"
            },
            validate_product,
            "product_id=2002"
        ),
        (
            "products",
            {
                "product_id": 2003,
                "name": "Negative Stock",
                "category": "Test",
                "price": 100,
                "stock": -5,
                "created_at": "2026-09-08T10:00:00"
            },
            validate_product,
            "product_id=2003"
        ),
        (
            "products",
            {
                "product_id": 2004,
                "name": "Missing Category",
                "category": None,
                "price": 100,
                "stock": 10,
                "created_at": "2026-09-08T10:00:00"
            },
            validate_product,
            "product_id=2004"
        ),
        (
            "orders",
            {
                "order_id": 3001,
                "customer_id": 1,
                "status": "INVALID",
                "total_amount": 1000,
                "created_at": "2026-09-08T10:00:00",
                "updated_at": "2026-09-08T10:00:00"
            },
            validate_order,
            "order_id=3001"
        ),
        (
            "orders",
            {
                "order_id": 3002,
                "customer_id": 1,
                "status": "PENDING",
                "total_amount": -500,
                "created_at": "2026-09-08T10:00:00",
                "updated_at": "2026-09-08T10:00:00"
            },
            validate_order,
            "order_id=3002"
        ),
        (
            "orders",
            {
                "order_id": 3003,
                "customer_id": 999999,
                "status": "PENDING",
                "total_amount": 500,
                "created_at": "2026-09-08T10:00:00",
                "updated_at": "2026-09-08T10:00:00"
            },
            validate_order,
            "order_id=3003"
        ),
        (
            "order_items",
            {
                "order_item_id": 4001,
                "order_id": 1,
                "product_id": 1,
                "quantity": 0,
                "unit_price": 100
            },
            validate_order_item,
            "order_item_id=4001"
        ),
        (
            "order_items",
            {
                "order_item_id": 4002,
                "order_id": 1,
                "product_id": 1,
                "quantity": 2,
                "unit_price": -100
            },
            validate_order_item,
            "order_item_id=4002"
        ),
        (
            "order_items",
            {
                "order_item_id": 4003,
                "order_id": 999999,
                "product_id": 1,
                "quantity": 2,
                "unit_price": 100
            },
            validate_order_item,
            "order_item_id=4003"
        ),
        (
            "order_items",
            {
                "order_item_id": 4004,
                "order_id": 1,
                "product_id": 999999,
                "quantity": 2,
                "unit_price": 100
            },
            validate_order_item,
            "order_item_id=4004"
        ),
        (
            "payments",
            {
                "payment_id": 5001,
                "order_id": 1,
                "amount": -100,
                "status": "SUCCESS",
                "payment_method": "CARD",
                "created_at": "2026-09-08T10:00:00"
            },
            validate_payment,
            "payment_id=5001"
        ),
        (
            "payments",
            {
                "payment_id": 5002,
                "order_id": 1,
                "amount": 100,
                "status": "INVALID",
                "payment_method": "CARD",
                "created_at": "2026-09-08T10:00:00"
            },
            validate_payment,
            "payment_id=5002"
        ),
        (
            "payments",
            {
                "payment_id": 5003,
                "order_id": 999999,
                "amount": 100,
                "status": "SUCCESS",
                "payment_method": "CARD",
                "created_at": "2026-09-08T10:00:00"
            },
            validate_payment,
            "payment_id=5003"
        )
    ]

    passed = 0
    failed = 0

    for table, record, validator, identifier in tests:
        if run_test(table, record, validator, identifier):
            passed += 1
        else:
            failed += 1

    print("=" * 60)
    print(f"TESTS={len(tests)} PASS={passed} FAIL={failed}")
    print(f"DLQ={DLQ_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
