# DLQ Investigation and Recovery Runbook

## 1. Purpose

This runbook describes how to investigate records rejected by StrataFlow data-quality validation, identify the failed rules, correct the underlying source data, and verify the data before any recovery attempt.

**Current capability:** StrataFlow captures DLQ test failures and produces data-quality and incident reports. An automated DLQ replay mechanism has not yet been implemented.

## 2. DLQ location

- DLQ file: `DataQuality/dlq/dlq_records.json`
- DQ report: `DataQuality/checks/dq_report.json`
- Incident reports: `AI/reports/`

Each DLQ entry contains:
- `table`: affected table
- `record`: rejected record
- `failed_rule`: failed validation rule
- `reason`: failure description
- `timestamp`: time the failure was recorded

A record can generate multiple DLQ entries when it violates multiple rules.

## 3. Identify the incident

Start by reviewing the available evidence:

1. Inspect the DLQ entries and group failures by table and rule.
2. Review `AI/reports/failure_patterns.json` for categorized failure patterns.
3. Review `AI/reports/final_incident_report.json` for reconciled incident facts, affected tables, scope and recommended action.
4. Check the latest DQ report to determine whether the current PostgreSQL data passes validation.

Do not assume that a passing DQ report means previously rejected DLQ records have been corrected. These are separate checks.

## 4. Understand the validation rules

The current validation scripts cover:

| Table | Validation examples |
|---|---|
| products | Required fields, non-negative price and stock |
| orders | Required fields, valid status, non-negative total, customer reference |
| order_items | Required fields, positive quantity, non-negative unit price, order and product references |
| payments | Required fields, valid status, non-negative amount, order reference |

Cross-table reconciliation also checks:
- Order total against the sum of its line items.
- Order total against successful payment amounts.

Refer to `DataQuality/checks/run_dq_checks.py` for the authoritative implemented SQL checks and `DataQuality/checks/run_dlq_test.py` for the test-record validators.

## 5. Investigate and correct

For each rejected record:

1. Identify its table and record identifier.
2. Read every associated `failed_rule` and `reason`.
3. Determine whether the problem is a missing value, invalid numeric value, invalid status, or missing parent record.
4. Trace the issue to the appropriate source system or upstream event.
5. Correct the source data using an approved process.
6. Verify referenced customer, order and product records exist before correcting foreign-key failures.
7. Preserve the original DLQ evidence for audit and investigation.

Do not blindly replace invalid values with defaults. Confirm the correct business value first.

## 6. Run data-quality validation

The implemented validation command is:

```powershell
python .\DataQuality\checks\run_dq_checks.py
```

It writes its results to `DataQuality/checks/dq_report.json` and exits with:
- `0` when all checks pass.
- `1` when one or more checks fail.

Run this only when PostgreSQL is available and you are ready to validate the current database state. It validates database tables; it does not independently validate or clear the DLQ file.

## 7. Replay safety

**Automated replay is not currently supported.**

The current implementation does not provide a verified process to:
- Revalidate individual DLQ records against the live database.
- Publish corrected records back to Kafka.
- Guarantee idempotent replay.
- Track replay attempts and outcomes.
- Remove or archive successfully replayed records.

Do not manually delete DLQ entries or treat a passing DQ report as permission to replay them.

Before implementing replay, the system should provide:
1. A record-level validation step.
2. Confirmed parent-record and business-rule integrity.
3. A controlled Kafka publishing mechanism.
4. Idempotency and duplicate protection.
5. Replay status, attempt count and failure tracking.
6. An audit trail and quarantine for records that fail again.

Only records that pass all required checks should become eligible for replay.

## 8. Important operational cautions

- `run_dlq_test.py` generates test failures and appends entries to the DLQ JSON file. Running it again can add duplicate test entries. Do not use it as a production recovery command.
- `run_dq_checks.py` checks current PostgreSQL data, not historical DLQ records.
- Preserve the original DLQ records and incident evidence.
- Do not claim an incident is recovered until corrected records have been independently verified and any implemented downstream recovery has been confirmed.

## 9. Current implementation boundary

| Capability | Status |
|---|---|
| Data-quality validation | Implemented |
| Test DLQ record generation | Implemented |
| DLQ JSON capture | Implemented |
| Failure categorization and incident investigation | Implemented |
| AI-assisted incident analysis | Implemented |
| Record-level DLQ revalidation | Not implemented |
| Automated Kafka DLQ replay | Not implemented |
| Replay audit and idempotency | Not implemented |

This runbook describes the current implementation, not a future-state replay design.
