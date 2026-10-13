import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


# ============================================================
# PulseFlow AI Incident Intelligence
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

# When running directly on Windows:
#   http://localhost:11434
#
# When running inside Docker:
#   http://host.docker.internal:11434
#
# Airflow will provide OLLAMA_HOST automatically.
OLLAMA_HOST = os.getenv(
    "OLLAMA_HOST",
    "http://localhost:11434"
).rstrip("/")

OLLAMA_URL = (
    f"{OLLAMA_HOST}/api/generate"
)

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
    print("PulseFlow AI Incident Intelligence")
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
            f"Invalid JSON file:\n{path}\n\n"
            f"{exc}"
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

    The prompt and output are deliberately kept small
    because qwen2.5-coder:7b is running locally on CPU.
    """

    payload = {
        "model": MODEL,

        "prompt": prompt,

        "stream": False,

        "format": "json",

        "options": {
            "temperature": 0,

            # Keep context very small.
            "num_ctx": 768,

            # Keep generated JSON very small.
            "num_predict": 120
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
    2. ```json fenced JSON
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

    # --------------------------------------------------------
    # Remove markdown code fences
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Attempt 1
    # Entire response is JSON
    # --------------------------------------------------------

    try:
        return json.loads(text)

    except json.JSONDecodeError:
        pass

    # --------------------------------------------------------
    # Attempt 2
    # Find outer JSON object
    # --------------------------------------------------------

    start = text.find("{")

    if start == -1:
        raise ValueError(
            "No JSON object found in Ollama response.\n\n"
            f"Raw response:\n{text}"
        )

    depth = 0
    in_string = False
    escape = False

    for index in range(
        start,
        len(text)
    ):

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
                    return json.loads(
                        candidate
                    )

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
    Create the smallest useful evidence package.

    IMPORTANT:
    Raw DLQ records are NOT sent to the LLM.

    The deterministic pipeline has already converted
    them into aggregated failure patterns.
    """

    summary = failure_patterns.get(
        "summary",
        {}
    )

    failure_categories = (
        failure_patterns.get(
            "failure_categories",
            {}
        )
    )

    affected_tables = (
        failure_patterns.get(
            "affected_tables",
            {}
        )
    )

    dominant_rule = (
        summary.get(
            "dominant_rule",
            {}
        )
    )

    dominant_category = (
        summary.get(
            "dominant_category",
            {}
        )
    )

    dq_summary = (
        incident_context.get(
            "dq_summary",
            {}
        )
    )

    evidence = {

        "dq": {
            "status": dq_summary.get(
                "status",
                "PASS"
            ),

            "checks": dq_summary.get(
                "total_checks",
                45
            ),

            "passed": dq_summary.get(
                "passed",
                45
            ),

            "failed": dq_summary.get(
                "failed",
                0
            )
        },

        "dlq_records": summary.get(
            "total_rejected_records",
            incident_context.get(
                "dlq_record_count",
                0
            )
        ),

        "pattern": summary.get(
            "pattern_type",
            "UNKNOWN"
        ),

        "categories": failure_categories,

        "tables": affected_tables,

        "dominant_rule": dominant_rule,

        "dominant_category": dominant_category,

        "deterministic_root_cause": (
            incident_report.get(
                "root_cause",
                ""
            )
        ),

        "deterministic_impact": (
            incident_report.get(
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
You are PulseFlow RCA.

Analyze ONLY this evidence:

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

Rules:
- Be concise.
- Do not invent facts.
- Use evidence only.
- confidence must be between 0 and 1.
- affected_records must match the evidence.
- scope must describe the actual affected scope from the evidence.
"""

    return prompt


# ============================================================
# AI Output Validation
# ============================================================

def validate_ai_analysis(analysis):

    if not isinstance(
        analysis,
        dict
    ):
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

    # --------------------------------------------------------
    # Confidence
    # --------------------------------------------------------

    confidence = analysis[
        "confidence"
    ]

    if not isinstance(
        confidence,
        (int, float)
    ):
        raise ValueError(
            "AI confidence must be numeric."
        )

    if confidence < 0 or confidence > 1:
        raise ValueError(
            "AI confidence must be between 0 and 1."
        )

    # --------------------------------------------------------
    # Affected records
    # --------------------------------------------------------

    affected_records = analysis[
        "affected_records"
    ]

    if not isinstance(
        affected_records,
        int
    ):
        raise ValueError(
            "affected_records must be an integer."
        )

    # --------------------------------------------------------
    # Replay
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Load pipeline artifacts
    # --------------------------------------------------------

    incident_context = load_json(
        INCIDENT_CONTEXT
    )

    incident_report = load_json(
        INCIDENT_REPORT
    )

    failure_patterns = load_json(
        FAILURE_PATTERNS
    )

    # --------------------------------------------------------
    # Build compact evidence
    # --------------------------------------------------------

    evidence = build_ai_evidence(
        incident_context,
        incident_report,
        failure_patterns
    )

    # --------------------------------------------------------
    # Build prompt
    # --------------------------------------------------------

    prompt = build_prompt(
        evidence
    )

    # --------------------------------------------------------
    # Diagnostics
    # --------------------------------------------------------

    print(
        f"Evidence size : "
        f"{len(json.dumps(evidence))} chars"
    )

    print(
        f"Prompt size   : "
        f"{len(prompt)} chars"
    )

    print()

    # --------------------------------------------------------
    # Call Ollama
    # --------------------------------------------------------

    raw_response = call_ollama(
        prompt
    )

    # --------------------------------------------------------
    # Show raw response
    # --------------------------------------------------------

    print(
        "--- RAW OLLAMA RESPONSE ---"
    )

    print(raw_response)

    print(
        "--- END RAW RESPONSE ---"
    )

    print()

    # --------------------------------------------------------
    # Parse JSON
    # --------------------------------------------------------

    ai_analysis = extract_json(
        raw_response
    )

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    ai_analysis = validate_ai_analysis(
        ai_analysis
    )

    # --------------------------------------------------------
    # Build final report
    # --------------------------------------------------------

    final_report = {

        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "pipeline": "PulseFlow",

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

    # --------------------------------------------------------
    # Save report
    # --------------------------------------------------------

    save_json(
        OUTPUT_FILE,
        final_report
    )

    # --------------------------------------------------------
    # Console summary
    # --------------------------------------------------------

    print("=" * 60)

    print(
        "AI INCIDENT ANALYSIS COMPLETED"
    )

    print("=" * 60)

    print(
        f"Incident type : "
        f"{ai_analysis['incident_type']}"
    )

    print(
        f"Root cause   : "
        f"{ai_analysis['probable_root_cause']}"
    )

    print(
        f"Confidence    : "
        f"{ai_analysis['confidence']}"
    )

    print(
        f"Scope         : "
        f"{ai_analysis['scope']}"
    )

    print(
        f"Records       : "
        f"{ai_analysis['affected_records']}"
    )

    print(
        f"Replay        : "
        f"{ai_analysis['replay_required']}"
    )

    print()

    print(
        f"Output        : "
        f"{OUTPUT_FILE}"
    )

    print("=" * 60)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()
