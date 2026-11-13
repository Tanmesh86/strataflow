try:
    from .embeddings import embed_text
    from .vector_store import search_documents
except ImportError:
    from embeddings import embed_text
    from vector_store import search_documents


def retrieve_context(query: str, top_k: int = 4, max_per_source: int = 2) -> str:
    """Retrieve relevant knowledge and format it for an LLM prompt."""

    if not query.strip():
        return ""

    results = search_documents(
        query_embedding=embed_text(query),
        top_k=top_k
    )

    selected = []
    source_counts = {}

    for result in results:
        source = result["metadata"].get("source", "unknown")
        count = source_counts.get(source, 0)

        if count >= max_per_source:
            continue

        source_counts[source] = count + 1
        selected.append(result)

    if not selected:
        return "No relevant historical knowledge was retrieved."

    sections = []

    for index, result in enumerate(selected, start=1):
        source = result["metadata"].get("source", "unknown")
        sections.append(
            f"[Knowledge {index} | Source: {source}]\n"
            f"{result['document']}"
        )

    return "\n\n".join(sections)


if __name__ == "__main__":
    question = (
        "How should failed DLQ records be investigated, "
        "and is automated replay implemented?"
    )

    print(retrieve_context(question))
