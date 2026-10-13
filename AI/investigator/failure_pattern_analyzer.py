import json
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone


BASE_DIR = Path(__file__).resolve().parents[2]

CONTEXT_FILE = BASE_DIR / "AI" / "reports" / "incident_context.json"

OUTPUT_FILE = BASE_DIR / "AI" / "reports" / "failure_patterns.json"


def load_context():

    if not CONTEXT_FILE.exists():
        raise FileNotFoundError(
            f"Missing incident context: {CONTEXT_FILE}"
        )

    with CONTEXT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


def classify_rule(rule):

    rule_lower = rule.lower()

    if (
        "reference" in rule_lower
        or "foreign" in rule_lower
        or "_id" in rule_lower
    ):
        return "REFERENTIAL_INTEGRITY"

    if (
        "must be >" in rule_lower
        or "must be >=" in rule_lower
    ):
        return "NUMERIC_CONSTRAINT"

    if "null" in rule_lower:
        return "NULL_CONSTRAINT"

    if (
        "valid" in rule_lower
        or "status" in rule_lower
    ):
        return "ENUM_CONSTRAINT"

    return "OTHER"


def analyze_patterns(context):

    rule_counts = Counter()

    category_counts = Counter()

    table_counts = Counter()

    table_rules = {}

    records = []

    for incident in context.get("incidents", []):

        if incident.get("source") != "dead_letter_queue":
            continue

        for item in incident.get("records", []):

            table = item.get(
                "table",
                "unknown"
            )

            rule = item.get(
                "failed_rule",
                "unknown"
            )

            category = classify_rule(rule)

            table_counts[table] += 1
            rule_counts[rule] += 1
            category_counts[category] += 1

            table_rules.setdefault(
                table,
                Counter()
            )

            table_rules[table][rule] += 1

            records.append({
                "table": table,
                "rule": rule,
                "category": category
            })

    total_records = len(records)

    affected_tables = len(table_counts)

    affected_categories = len(category_counts)

    dominant_rule = None

    if rule_counts:

        rule, count = rule_counts.most_common(1)[0]

        dominant_rule = {
            "rule": rule,
            "count": count,
            "percentage": round(
                count / total_records * 100,
                2
            )
        }

    dominant_category = None

    if category_counts:

        category, count = category_counts.most_common(1)[0]

        dominant_category = {
            "category": category,
            "count": count,
            "percentage": round(
                count / total_records * 100,
                2
            )
        }

    pattern_type = "MIXED_FAILURES"

    if affected_categories == 1:

        pattern_type = (
            next(iter(category_counts))
        )

    elif affected_tables >= 3:

        pattern_type = "CROSS_TABLE_FAILURE"

    table_summary = {}

    for table, rules in table_rules.items():

        table_summary[table] = {
            "record_count": table_counts[table],
            "rules": dict(rules)
        }

    return {

        "generated_at":
            datetime.now(timezone.utc).isoformat(),

        "pipeline": "PulseFlow",

        "summary": {

            "total_rejected_records":
                total_records,

            "affected_tables":
                affected_tables,

            "affected_failure_categories":
                affected_categories,

            "pattern_type":
                pattern_type,

            "dominant_rule":
                dominant_rule,

            "dominant_category":
                dominant_category
        },

        "failure_categories":
            dict(category_counts),

        "failure_rules":
            dict(rule_counts),

        "affected_tables":
            dict(table_counts),

        "table_details":
            table_summary
    }


def main():

    context = load_context()

    patterns = analyze_patterns(context)

    with OUTPUT_FILE.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            patterns,
            file,
            indent=2
        )

    print("=" * 60)
    print("PulseFlow Failure Pattern Analyzer")
    print("=" * 60)

    print(
        f"Rejected records : "
        f"{patterns['summary']['total_rejected_records']}"
    )

    print(
        f"Affected tables  : "
        f"{patterns['summary']['affected_tables']}"
    )

    print(
        f"Pattern type     : "
        f"{patterns['summary']['pattern_type']}"
    )

    print(
        f"Dominant rule    : "
        f"{patterns['summary']['dominant_rule']}"
    )

    print(
        f"Dominant category: "
        f"{patterns['summary']['dominant_category']}"
    )

    print(
        f"Output           : "
        f"{OUTPUT_FILE}"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()