# Historical Incident: Cross-Table Data Quality Failure

## 1. Incident metadata

- Pipeline name in original report: PulseFlow (legacy project name)
- Project: StrataFlow
- Incident date: 2026-11-09
- Report type: Evidence-Reconciled AI Incident Report
- Severity: MEDIUM
- Incident type: CROSS_TABLE_FAILURE
- Reconciliation status: RECONCILED

## 2. Incident summary

A total of 13 records failed data-quality validation and were captured in the Dead Letter Queue. The failures affected four tables: products, orders, order_items and payments.

The records were prevented from progressing through the validated pipeline.

## 3. Deterministic evidence

| Affected table | Rejected records |
|---|---:|
| products | 3 |
| orders | 3 |
| order_items | 4 |
| payments | 3 |
| **Total** | **13** |

The most frequent individual validation rule was:

`order_id must reference an existing order`

- Occurrences: 2
- Share of recorded failures: 15.38%

This is the dominant individual rule, not the overall root cause.

## 4. Failure categories

The original failure-pattern report classified the rejected records into:

- NUMERIC_CONSTRAINT
- NULL_CONSTRAINT
- ENUM_CONSTRAINT
- REFERENTIAL_INTEGRITY

These categories indicate invalid numeric values, missing required values, invalid enumerated statuses and broken references.

## 5. Root cause and impact

**Deterministic root cause:**

13 records failed validation and were captured in the Dead Letter Queue.

**Impact:**

13 invalid records were prevented from progressing through the validated pipeline.

The available report does not establish that these records were subsequently corrected or replayed.

## 6. AI analysis and reconciliation

The original AI analysis reported:

- Incident type: CROSS_TABLE_FAILURE
- Confidence: 0.95
- Affected records: 13
- Affected tables: products, orders, order_items, payments
- Reconciliation mismatches: 0

The reconciliation process used deterministic failure-pattern evidence as the trusted source and allowed deterministic facts to override AI output.

The confidence value is the historical model output, not an independently calibrated probability.

## 7. Recommended investigation

Based on the recorded failures:

1. Inspect rejected records and their individual validation reasons.
2. Prioritize numeric, nullability, enum and referential-integrity violations.
3. Verify customer, order and product references.
4. Correct the upstream data using verified business values.
5. Run the implemented PostgreSQL data-quality checks.
6. Preserve the DLQ evidence until a safe recovery process is available.

## 8. Recovery status

The historical report recommends correction followed by replay after successful validation.

However, the inspected StrataFlow implementation does not currently contain an automated DLQ replay mechanism.

Therefore:
- Incident evidence is available.
- Deterministic and AI analysis were reconciled.
- Successful correction is not established by this report.
- Successful replay is not established by this report.

## 9. Source of truth

Original source:

`AI/reports/final_incident_report.json`

This document is a knowledge-base summary of that report. For exact incident evidence, refer to the original JSON artifact.
