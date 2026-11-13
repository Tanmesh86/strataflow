
import os
from pathlib import Path

import chromadb


BASE_DIR = Path(__file__).resolve().parents[2]

VECTOR_STORE_DIR = Path(
    os.getenv(
        "RAG_VECTOR_STORE_DIR",
        str(BASE_DIR / "AI" / "vector_store")
    )
)

# Versioned collection. The original v1 collection remains untouched.
COLLECTION_NAME = os.getenv(
    "RAG_COLLECTION_NAME",
    "strataflow_knowledge_v2"
)


def get_collection():
    """Open or create the persistent StrataFlow knowledge collection."""

    client = chromadb.PersistentClient(
        path=str(VECTOR_STORE_DIR)
    )

    return client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={
            "description": "StrataFlow incident and operational knowledge",
            "hnsw:space": "cosine"
        }
    )


def add_documents(
    ids: list[str],
    documents: list[str],
    embeddings: list[list[float]],
    metadatas: list[dict]
):
    """Insert or update document chunks in the collection."""

    if not (
        len(ids) == len(documents)
        == len(embeddings) == len(metadatas)
    ):
        raise ValueError("All input lists must have the same length.")

    if not ids:
        return

    if any(not document.strip() for document in documents):
        raise ValueError("Documents must not contain empty text.")

    if any(len(vector) != 768 for vector in embeddings):
        raise ValueError("All embeddings must contain 768 dimensions.")

    collection = get_collection()

    collection.upsert(
        ids=ids,
        documents=documents,
        embeddings=embeddings,
        metadatas=metadatas
    )


def search_documents(
    query_embedding: list[float],
    top_k: int = 3
) -> list[dict]:
    """Retrieve the most relevant knowledge chunks."""

    if len(query_embedding) != 768:
        raise ValueError("Query embedding must contain 768 dimensions.")

    if top_k < 1:
        raise ValueError("top_k must be at least 1.")

    collection = get_collection()

    if collection.count() == 0:
        return []

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=min(top_k, collection.count()),
        include=["documents", "metadatas", "distances"]
    )

    matches = []

    for index, document in enumerate(results["documents"][0]):
        matches.append({
            "document": document,
            "metadata": results["metadatas"][0][index],
            "distance": results["distances"][0][index]
        })

    return matches


if __name__ == "__main__":
    collection = get_collection()

    print(f"Collection: {COLLECTION_NAME}")
    print(f"Storage: {VECTOR_STORE_DIR}")
    print(f"Existing chunks: {collection.count()}")
