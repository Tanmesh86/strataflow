
from langgraph.graph import END, START, StateGraph

from AI.orchestration.state import IncidentState
from AI.orchestration.nodes import (
    load_inputs,
    analyze_incident,
    build_evidence,
    retrieve_knowledge,
    analyze_with_llm,
    save_report,
)


def build_incident_graph():
    """Build the StrataFlow incident investigation workflow."""

    graph = StateGraph(IncidentState)

    graph.add_node("load_inputs", load_inputs)
    graph.add_node("analyze_incident", analyze_incident)
    graph.add_node("build_evidence", build_evidence)
    graph.add_node("retrieve_knowledge", retrieve_knowledge)
    graph.add_node("analyze_with_llm", analyze_with_llm)
    graph.add_node("save_report", save_report)

    graph.add_edge(START, "load_inputs")
    graph.add_edge("load_inputs", "analyze_incident")
    graph.add_edge("analyze_incident", "build_evidence")
    graph.add_edge("build_evidence", "retrieve_knowledge")
    graph.add_edge("retrieve_knowledge", "analyze_with_llm")
    graph.add_edge("analyze_with_llm", "save_report")
    graph.add_edge("save_report", END)

    return graph.compile()


incident_graph = build_incident_graph()


if __name__ == "__main__":
    result = incident_graph.invoke({})

    print("\n" + "=" * 60)
    print("STRATAFLOW LANGGRAPH INCIDENT INVESTIGATION")
    print("=" * 60)
    print("Pipeline:", result["final_report"]["pipeline"])
    print("Incident:", result["ai_analysis"]["incident_type"])
    print("Affected records:", result["ai_analysis"]["affected_records"])
    print("AI confidence:", result["ai_analysis"]["confidence"])
    print("Reconciliation:", result["ai_reconciliation"]["status"])
    print("Report:", result["output_path"])
