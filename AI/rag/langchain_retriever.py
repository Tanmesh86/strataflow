from langchain_core.embeddings import Embeddings
from langchain_chroma import Chroma

try:
    from .embeddings import embed_text
    from .vector_store import (
        COLLECTION_NAME,
        VECTOR_STORE_DIR,
        get_collection,
    )
except ImportError:
    from embeddings import embed_text
    from vector_store import (
        COLLECTION_NAME,
        VECTOR_STORE_DIR,
        get_collection,
    )


class OllamaNomicEmbeddings(Embeddings):
    """LangChain-compatible adapter for the existing Ollama embeddings."""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [embed_text(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return embed_text(text)


def get_langchain_vector_store() -> Chroma:
    """Connect LangChain to the existing persistent Chroma collection."""

    return Chroma(
        collection_name=COLLECTION_NAME,
        persist_directory=str(VECTOR_STORE_DIR),
        embedding_function=OllamaNomicEmbeddings(),
    )


def retrieve_documents(query: str, top_k: int = 4):
    """Retrieve existing knowledge chunks through LangChain."""

    if not query or not query.strip():
        return []

    if top_k < 1:
        raise ValueError("top_k must be at least 1.")

    vector_store = get_langchain_vector_store()

    return vector_store.similarity_search_with_score(
        query,
        k=top_k
    )


if __name__ == "__main__":
    before_count = get_collection().count()

    question = (
        "How should failed DLQ records be investigated, "
        "and is automated replay implemented?"
    )

    print("Collection:", COLLECTION_NAME)
    print("Documents before search:", before_count)

    results = retrieve_documents(question, top_k=4)

    print("Retrieved documents:", len(results))

    for index, (document, distance) in enumerate(results, start=1):
        print(f"\n--- Result {index} ---")
        print("Source:", document.metadata.get("source", "unknown"))
        print("Distance:", distance)
        print("Content:", document.page_content[:400])

    after_count = get_collection().count()
    print("\nDocuments after search:", after_count)

    if before_count != after_count:
        raise RuntimeError("Unexpected change in Chroma document count.")

    print("PASS: Existing collection remains unchanged.")
