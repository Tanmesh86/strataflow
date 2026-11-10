
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


# ============================================================
# StrataFlow AI Incident Intelligence
# Local LLM Root Cause Analysis
# CPU-Optimized Version
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]

INCIDENT_CONTEXT = (
    BASE_DIR
    / "AI"
    / "reports"
    / "incident_context.json"
)

INCIDENT_REPORT = (
    BASE_DIR
    / "AI"
    / "reports"
    / "incident_report.json"
)

FAILURE_PATTERNS = (
    BASE_DIR
    / "AI"
    / "reports"
    / "failure_patterns.json"
)

OUTPUT_FILE = (
    BASE_DIR
    / "AI"
    / "reports"
    / "ai_incident_report.json"
)


# ============================================================
# Ollama Configuration
# ============================================================

OLLAMA_HOST = os.getenv(
    "OLLAMA_HOST",
    "http://localhost:11434"
).rstrip("/")

OLLAMA_URL = f"{OLLAMA_HOST}/api/generate"

MODEL = os.getenv(
    "OLLAMA_MODEL",
    "qwen2.5-coder:7b"
)

OLLAMA_TIMEOUT = int(
    os.getenv(
        "OLLAMA_TIMEOUT",
        "90"
    )
)


# ============================================================
# Console Helpers
# ============================================================

def print_header():
    print("=" * 60)
    print("StrataFlow AI Incident Intelligence")
    print("=" * 60)

    print(f"Model       : {MODEL}")
    print(f"Ollama      : {OLLAMA_HOST}")
    print("Pattern data: failure_patterns.json")
    print("Sending     : Ollama")
    print()


# ============================================================
# JSON File Utilities
# ============================================================

def load_json(path):

    if not path.exists():
        raise FileNotFoundError(
            f"Required file not found:\n{path}"
        )

    try:
        with open(
            path,
            "r",
            encoding="utf-8"
        ) as file:
            return json.load(file)

    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON file:\n{path}\n\n{exc}"
        ) from exc


def save_json(path, data):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            data,
            file,
            indent=2
        )

    return path


# ============================================================
# Ollama API
# ============================================================

def call_ollama(prompt):
    """
    CPU-optimized Ollama request.

    Uses deterministic generation and a compact response.
    """

    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",

        "options": {
            "temperature": 0,
            "num_ctx": 768,
            "num_predict": 180
        }
    }

    data = json.dumps(
        payload,
        separators=(",", ":")
    ).encode("utf-8")

    request = Request(
        OLLAMA_URL,
        data=data,
        headers={
            "Content-Type": "application/json"
        },
        method="POST"
    )

    try:
        with urlopen(
            request,
            timeout=OLLAMA_TIMEOUT
        ) as response:

            raw = response.read().decode(
                "utf-8"
            )

            result = json.loads(raw)

    except Exception as exc:
        raise RuntimeError(
            "Failed to communicate with Ollama: "
            f"{exc}"
        ) from exc

    if "response" not in result:
        raise RuntimeError(
            "Ollama response did not contain "
            "'response'.\n\n"
            f"Response:\n{result}"
        )

    return result["response"]


# ============================================================
# Robust JSON Extraction
# ============================================================

def extract_json(text):
    """
    Extract JSON safely from an LLM response.

    Handles:
    1. Pure JSON
    2. Markdown fenced JSON
    3. Text surrounding JSON
    4. Nested objects
    """

    if text is None:
        raise ValueError(
            "Ollama returned None."
        )

    text = str(text).strip()

    if not text:
        raise ValueError(
            "Ollama returned an empty response."
        )

    # Remove markdown code fences.
    if text.startswith("```"):

        text = re.sub(
            r"^```(?:json)?\s*",
            "",
            text,
            flags=re.IGNORECASE
        )

        text = re.sub(
            r"\s*```$",
            "",
            text
        )

        text = text.strip()

    # Attempt 1: entire response is JSON.
    try:
        return json.loads(text)

    except json.JSONDecodeError:
        pass

    # Attempt 2: extract outer JSON object.
    start = text.find("{")

    if start == -1:
        raise ValueError(
            "No JSON object found in Ollama response.\n\n"
            f"Raw response:\n{text}"
        )

    depth = 0
    in_string = False
    escape = False

    for index in range(start, len(text)):

        char = text[index]

        if in_string:

            if escape:
                escape = False

            elif char == "\\":
                escape = True

            elif char == '"':
                in_string = False

            continue

        if char == '"':
            in_string = True

        elif char == "{":
            depth += 1

        elif char == "}":
            depth -= 1

            if depth == 0:

                candidate = text[
                    start:index + 1
                ]

                try:
                    return json.loads(candidate)

                except json.JSONDecodeError as exc:
                    raise ValueError(
                        "Ollama returned malformed JSON.\n\n"
                        f"JSON error: {exc}\n\n"
                        f"Response:\n{candidate}"
                    ) from exc

    raise ValueError(
        "Could not find a complete JSON object "
        "in Ollama response.\n\n"
        f"Raw response:\n{text}"
    )


# ============================================================
# Compact Evidence Builder
# ============================================================

def build_ai_evidence(
    incident_context,
    incident_report,
    failure_patterns
):
    """
    Build a compact, deterministic evidence package.

    Raw DLQ records are not sent to the LLM.

    Aggregated failure patterns and deterministic
    investigation facts are used instead.
    """

    summary = failure_patterns.get(
        "summary",
        {}
    )

    failure_categories = failure_patterns.get(
        "failure_categories",
        {}
    )

    affected_tables = failure_patterns.get(
        "affected_tables",
        {}
    )

    dominant_rule = summary.get(
        "dominant_rule",
        {}
    )

    dominant_category = summary.get(
        "dominant_category",
        {}
    )

    dq_summary = incident_context.get(
        "dq_summary",
        {}
    )

    # IMPORTANT:
    # Deterministic investigation is nested inside
    # incident_report.json under "investigation".
    deterministic_investigation = (
        incident_report.get(
            "investigation",
            incident_report
        )
    )

    # Correct DLQ fallback:
    # incident_context stores the count under
    # dlq_summary.record_count, not dlq_record_count.
    context_dlq_summary = incident_context.get(
        "dlq_summary",
        {}
    )

    total_rejected_records = summary.get(
        "total_rejected_records",
        context_dlq_summary.get(
            "record_count",
            0
        )
    )

    evidence = {

        "dq": {
            "status": dq_summary.get(
                "status",
                "UNKNOWN"
            ),

            "checks": dq_summary.get(
                "total_checks",
                0
            ),

            "passed": dq_summary.get(
                "passed",
                0
            ),

            "failed": dq_summary.get(
                "failed",
                0
            )
        },

        # Overall incident count.
        "dlq_records": total_rejected_records,

        "pattern": summary.get(
            "pattern_type",
            "UNKNOWN"
        ),

        "categories": failure_categories,

        # All affected tables and their record counts.
        "tables": affected_tables,

        # Most frequent individual validation failure.
        "dominant_rule": dominant_rule,

        "dominant_category": dominant_category,

        # Overall deterministic investigation facts.
        "deterministic_root_cause": (
            deterministic_investigation.get(
                "root_cause",
                ""
            )
        ),

        "deterministic_impact": (
            deterministic_investigation.get(
                "impact",
                ""
            )
        )
    }

    return evidence


# ============================================================
# AI Prompt
# ============================================================

def build_prompt(evidence):

    evidence_json = json.dumps(
        evidence,
        separators=(",", ":")
    )

    prompt = f"""
You are StrataFlow RCA, a data engineering
incident investigation assistant.

Analyze ONLY the supplied evidence.

EVIDENCE:
{evidence_json}

Return ONLY valid JSON.

Use exactly this structure:

{{
"incident_type":"",
"probable_root_cause":"",
"confidence":0.0,
"scope":"",
"affected_records":0,
"impact":"",
"recommended_action":"",
"replay_required":false
}}

RULES:

1. Do not invent facts. Use evidence only.

2. Confidence must be a number between 0 and 1.
   It is an estimate, not a calibrated probability.

3. affected_records MUST equal dlq_records.
   dlq_records represents the TOTAL number of
   rejected records across the entire incident.

4. scope MUST include ALL affected tables listed
   in the tables field.

5. Return scope as a comma-separated string
   containing every affected table name.

6. dominant_rule.count represents only the number
   of records matching the most frequent individual
   validation rule. It is NOT the total incident
   record count.

7. Do not confuse the dominant validation rule
   with the overall incident root cause.

8. probable_root_cause must describe the overall
   incident using deterministic_root_cause and
   the supplied failure patterns.

9. incident_type MUST match the pattern field.

10. replay_required must be true when rejected
    records require correction and replay.

11. Keep every JSON value concise.
"""

    return prompt


# ============================================================
# AI Output Validation
# ============================================================

def validate_ai_analysis(analysis):

    if not isinstance(analysis, dict):
        raise ValueError(
            "AI response is not a JSON object."
        )

    required_fields = [
        "incident_type",
        "probable_root_cause",
        "confidence",
        "scope",
        "affected_records",
        "impact",
        "recommended_action",
        "replay_required"
    ]

    missing = [
        field
        for field in required_fields
        if field not in analysis
    ]

    if missing:
        raise ValueError(
            "AI response is missing fields: "
            + ", ".join(missing)
        )

    # Validate text fields.
    text_fields = [
        "incident_type",
        "probable_root_cause",
        "scope",
        "impact",
        "recommended_action"
    ]

    for field in text_fields:

        if not isinstance(
            analysis[field],
            str
        ):
            raise ValueError(
                f"{field} must be a string."
            )

        if not analysis[field].strip():
            raise ValueError(
                f"{field} cannot be empty."
            )

    # Validate confidence.
    confidence = analysis["confidence"]

    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not math.isfinite(confidence)
    ):
        raise ValueError(
            "AI confidence must be a finite number."
        )

    if confidence < 0 or confidence > 1:
        raise ValueError(
            "AI confidence must be between 0 and 1."
        )

    # Validate affected record count.
    affected_records = analysis["affected_records"]

    if (
        isinstance(affected_records, bool)
        or not isinstance(affected_records, int)
        or affected_records < 0
    ):
        raise ValueError(
            "affected_records must be a non-negative integer."
        )

    # Validate replay flag.
    if not isinstance(
        analysis["replay_required"],
        bool
    ):
        raise ValueError(
            "replay_required must be boolean."
        )

    return analysis


# ============================================================
# Main
# ============================================================

def main():

    print_header()

    # Load pipeline artifacts.
    incident_context = load_json(
        INCIDENT_CONTEXT
    )

    incident_report = load_json(
        INCIDENT_REPORT
    )

    failure_patterns = load_json(
        FAILURE_PATTERNS
    )

    # Build compact evidence.
    evidence = build_ai_evidence(
        incident_context,
        incident_report,
        failure_patterns
    )

    # Build prompt.
    prompt = build_prompt(
        evidence
    )

    # Diagnostics.
    print(
        f"Evidence size : {len(json.dumps(evidence))} chars"
    )

    print(
        f"Prompt size   : {len(prompt)} chars"
    )

    print()

    print("--- DETERMINISTIC EVIDENCE ---")

    print(
        json.dumps(
            evidence,
            indent=2
        )
    )

    print()

    # Call Ollama.
    raw_response = call_ollama(prompt)

    print("--- RAW OLLAMA RESPONSE ---")
    print(raw_response)
    print("--- END RAW RESPONSE ---")
    print()

    # Parse JSON.
    ai_analysis = extract_json(raw_response)

    # Validate output structure and types.
    ai_analysis = validate_ai_analysis(
        ai_analysis
    )

    # Build final AI report.
    final_report = {

        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "pipeline": "StrataFlow",

        "engine": {
            "type": "local_llm",
            "provider": "Ollama",
            "model": MODEL,
            "endpoint": OLLAMA_HOST
        },

        "evidence": evidence,

        "deterministic_investigation": (
            incident_report
        ),

        "ai_analysis": ai_analysis
    }

    # Save AI report.
    save_json(
        OUTPUT_FILE,
        final_report
    )

    # Console summary.
    print("=" * 60)
    print("AI INCIDENT ANALYSIS COMPLETED")
    print("=" * 60)

    print(
        f"Incident type : {ai_analysis['incident_type']}"
    )

    print(
        f"Root cause    : {ai_analysis['probable_root_cause']}"
    )

    print(
        f"Confidence    : {ai_analysis['confidence']}"
    )

    print(
        f"Scope         : {ai_analysis['scope']}"
    )

    print(
        f"Records       : {ai_analysis['affected_records']}"
    )

    print(
        f"Replay        : {ai_analysis['replay_required']}"
    )

    print()

    print(f"Output        : {OUTPUT_FILE}")

    print("=" * 60)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()
