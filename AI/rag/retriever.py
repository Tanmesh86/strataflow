try:
    from .langchain_retriever import retrieve_documents
except ImportError:
    from langchain_retriever import retrieve_documents


def retrieve_context(
    query: str,
    top_k: int = 4,
    max_per_source: int = 2
) -> str:
    """Retrieve and format knowledge using LangChain and Chroma."""

    if not query or not query.strip():
        return ""

    if max_per_source < 1:
        raise ValueError("max_per_source must be at least 1.")

    results = retrieve_documents(
        query=query,
        top_k=top_k
    )

    selected = []
    source_counts = {}

    for document, distance in results:
        source = document.metadata.get("source", "unknown")

        if source_counts.get(source, 0) >= max_per_source:
            continue

        source_counts[source] = source_counts.get(source, 0) + 1

        selected.append({
            "document": document.page_content,
            "source": source,
            "distance": distance
        })

    if not selected:
        return "No relevant historical knowledge was retrieved."

    sections = []

    for index, result in enumerate(selected, start=1):
        sections.append(
            f"[Knowledge {index} | Source: {result['source']}]\n"
            f"{result['document']}"
        )

    return "\n\n".join(sections)


if __name__ == "__main__":
    question = (
        "How should failed DLQ records be investigated, "
        "and is automated replay implemented?"
    )

    print(retrieve_context(question))
