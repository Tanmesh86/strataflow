
import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Add project root to Python path
BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))

from AI.rag.retriever import retrieve_context


REPORT_FILE = BASE_DIR / "AI" / "reports" / "langgraph_incident_report.json"


st.set_page_config(
    page_title="StrataFlow | Incident Intelligence",
    page_icon="🔎",
    layout="wide"
)


def load_report():
    if not REPORT_FILE.exists():
        return None

    with REPORT_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def show_json(data):
    st.json(data if data else {})


# Sidebar
st.sidebar.title("StrataFlow")
st.sidebar.caption("Data Platform Incident Intelligence")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    [
        "Incident Overview",
        "Failure Analysis",
        "AI Investigation",
        "Reconciliation",
        "Knowledge Search"
    ]
)

if st.sidebar.button("Refresh report"):
    st.cache_data.clear()
    st.rerun()


@st.cache_data
def get_report():
    return load_report()


report = get_report()

st.title("🔎 StrataFlow Incident Intelligence")
st.caption("Evidence-driven monitoring, investigation and AI-assisted root cause analysis")

if report is None:
    st.error("Incident report not found. Run the LangGraph investigation first.")
    st.stop()


evidence = report.get("evidence", {})
investigation = report.get("deterministic_investigation", {})
ai = report.get("ai_analysis", {})
reconciliation = report.get("ai_reconciliation", {})
engine = report.get("engine", {})

# -------------------------------------------------------------------
# INCIDENT OVERVIEW
# -------------------------------------------------------------------
if page == "Incident Overview":

    st.subheader("Latest Pipeline Incident")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Incident Severity", investigation.get("severity", "N/A"))
    col2.metric("DLQ Records", evidence.get("dlq_records", 0))
    col3.metric("DQ Checks Passed", f'{evidence.get("dq", {}).get("passed", 0)}/{evidence.get("dq", {}).get("checks", 0)}')
    col4.metric("AI Confidence", f'{ai.get("confidence", 0) * 100:.0f}%')

    st.divider()

    st.markdown("### Incident Summary")
    st.write(investigation.get("title", "No incident title available"))
    st.write(investigation.get("root_cause", "No root cause available"))

    st.markdown("### Business Impact")
    st.info(investigation.get("impact", "No impact information available"))

    st.markdown("### Affected Tables")

    tables = evidence.get("tables", {})
    if tables:
        table_df = pd.DataFrame(
            list(tables.items()),
            columns=["Table", "Rejected Records"]
        )
        st.bar_chart(table_df.set_index("Table"))

    st.markdown("### Investigation Recommendation")
    st.warning(investigation.get("recommendation", "No recommendation available"))

    st.caption(f"Report generated: {report.get('generated_at', 'Unknown')}")

# -------------------------------------------------------------------
# FAILURE ANALYSIS
# -------------------------------------------------------------------
elif page == "Failure Analysis":

    st.subheader("Data Quality Failure Analysis")

    categories = evidence.get("categories", {})
    tables = evidence.get("tables", {})
    dominant_rule = evidence.get("dominant_rule", {})
    dominant_category = evidence.get("dominant_category", {})

    left, right = st.columns(2)

    with left:
        st.markdown("### Failure Categories")
        if categories:
            category_df = pd.DataFrame(
                list(categories.items()),
                columns=["Category", "Count"]
            )
            st.bar_chart(category_df.set_index("Category"))

    with right:
        st.markdown("### Affected Tables")
        if tables:
            table_df = pd.DataFrame(
                list(tables.items()),
                columns=["Table", "Count"]
            )
            st.bar_chart(table_df.set_index("Table"))

    st.divider()

    st.markdown("### Dominant Failure Category")
    st.metric(
        dominant_category.get("category", "N/A"),
        dominant_category.get("count", 0),
        f'{dominant_category.get("percentage", 0)}% of rejected records'
    )

    st.markdown("### Most Frequent Individual Rule")
    st.write(dominant_rule.get("rule", "N/A"))
    st.metric(
        "Occurrences",
        dominant_rule.get("count", 0),
        f'{dominant_rule.get("percentage", 0)}% of recorded failures'
    )

    st.caption(
        "The most frequent individual rule is not necessarily "
        "the overall root cause."
    )

# -------------------------------------------------------------------
# AI INVESTIGATION
# -------------------------------------------------------------------
elif page == "AI Investigation":

    st.subheader("LLM-Powered Root Cause Analysis")

    st.write("**Provider:**", engine.get("provider", "N/A"))
    st.write("**Model:**", engine.get("model", "N/A"))
    st.write("**Framework:** LangGraph")

    st.divider()

    st.markdown("### AI Analysis")

    st.write("**Incident Type:**", ai.get("incident_type", "N/A"))
    st.write("**Probable Root Cause:**", ai.get("probable_root_cause", "N/A"))
    st.write("**Affected Records:**", ai.get("affected_records", 0))
    st.write("**Affected Tables:**", ai.get("scope", "N/A"))

    confidence = ai.get("confidence", 0)
    st.progress(max(0.0, min(float(confidence), 1.0)))
    st.caption(f"Model-reported confidence: {confidence}")

    st.markdown("### Recommended Action")
    st.info(ai.get("recommended_action", "No recommendation available"))

    st.markdown("### Replay Assessment")
    if ai.get("replay_required"):
        st.warning("AI indicates replay may be required. This dashboard does not execute replay.")
    else:
        st.write("AI did not recommend replay for this incident.")

    st.caption("AI output is advisory and must be validated against deterministic evidence.")

# -------------------------------------------------------------------
# RECONCILIATION
# -------------------------------------------------------------------
elif page == "Reconciliation":

    st.subheader("Deterministic vs AI Reconciliation")

    status = reconciliation.get("status", "UNKNOWN")

    if status == "RECONCILED":
        st.success(f"Status: {status}")
    else:
        st.warning(f"Status: {status}")

    checks = reconciliation.get("checks", {})

    for check, passed in checks.items():
        if passed:
            st.success(f"✓ {check}")
        else:
            st.error(f"✗ {check}")

    st.markdown("### Mismatches")
    mismatches = reconciliation.get("mismatches", [])

    if mismatches:
        st.dataframe(pd.DataFrame(mismatches), use_container_width=True)
    else:
        st.success("No reconciliation mismatches found.")

    st.markdown("### Full Reconciliation Details")
    show_json(reconciliation)

# -------------------------------------------------------------------
# RAG KNOWLEDGE SEARCH
# -------------------------------------------------------------------
elif page == "Knowledge Search":

    st.subheader("Historical Incident Knowledge Search")
    st.caption("Search previously indexed incident reports and recovery knowledge.")

    query = st.text_input(
        "Describe the failure or investigation topic",
        placeholder="e.g. How should referential integrity failures be investigated?"
    )

    top_k = st.slider("Number of retrieved chunks", 1, 5, 3)

    if st.button("Search Knowledge", type="primary"):

        if not query.strip():
            st.warning("Enter a search query first.")
        else:
            with st.spinner("Retrieving relevant knowledge..."):
                try:
                    result = retrieve_context(
                        query.strip(),
                        top_k=top_k,
                        max_per_source=2
                    )
                    st.markdown("### Retrieved Knowledge")
                    st.markdown(result if result else "No relevant knowledge found.")
                except Exception as error:
                    st.error(f"Knowledge retrieval failed: {error}")


st.divider()
st.caption("StrataFlow | LangGraph + Ollama + RAG | Local Incident Intelligence")
