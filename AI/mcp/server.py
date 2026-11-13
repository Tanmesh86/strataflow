
import json
import asyncio
from pathlib import Path

from mcp.server.mcpserver import MCPServer

from AI.rag.retriever import retrieve_context


BASE_DIR = Path(__file__).resolve().parents[2]
REPORT_FILE = BASE_DIR / "AI" / "reports" / "langgraph_incident_report.json"


server = MCPServer(
    name="strataflow",
    version="1.0.0",
    instructions=(
        "StrataFlow incident intelligence server. "
        "Provides read-only incident analysis, failure patterns, "
        "AI reconciliation and historical knowledge retrieval."
    )
)


def load_report():
    """Load the latest LangGraph incident report."""
    if not REPORT_FILE.exists():
        raise FileNotFoundError(
            f"Incident report not found: {REPORT_FILE}"
        )

    with REPORT_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


@server.tool()
def get_latest_incident() -> dict:
    """Get the latest incident and deterministic investigation details."""
    report = load_report()

    return {
        "generated_at": report.get("generated_at"),
        "pipeline": report.get("pipeline"),
        "investigation": report.get("deterministic_investigation"),
        "evidence": report.get("evidence")
    }


@server.tool()
def get_failure_patterns() -> dict:
    """Get failure categories, affected tables and dominant failure rules."""
    report = load_report()
    evidence = report.get("evidence", {})

    return {
        "pattern": evidence.get("pattern"),
        "categories": evidence.get("categories"),
        "tables": evidence.get("tables"),
        "dominant_rule": evidence.get("dominant_rule"),
        "dominant_category": evidence.get("dominant_category"),
        "dlq_records": evidence.get("dlq_records")
    }


@server.tool()
def get_ai_analysis() -> dict:
    """Get the latest LLM incident analysis and recommendations."""
    report = load_report()

    return {
        "engine": report.get("engine"),
        "analysis": report.get("ai_analysis")
    }


@server.tool()
def get_reconciliation() -> dict:
    """Get deterministic versus AI reconciliation results."""
    report = load_report()

    return report.get("ai_reconciliation", {})


@server.tool()
def search_knowledge(query: str) -> str:
    """Search StrataFlow's historical incident knowledge base."""
    if not query or not query.strip():
        return "Please provide a non-empty search query."

    return retrieve_context(
        query.strip(),
        top_k=3,
        max_per_source=2
    )


def main():
    print(
        "Starting StrataFlow MCP server...",
        flush=True
    )

    asyncio.run(server.run_stdio_async())


if __name__ == "__main__":
    main()
