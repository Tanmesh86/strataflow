import os
import requests


OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
EMBEDDING_MODEL = os.getenv("RAG_EMBEDDING_MODEL", "nomic-embed-text")
EMBEDDING_URL = f"{OLLAMA_HOST}/api/embeddings"

EXPECTED_DIMENSIONS = 768
TIMEOUT = 60


def embed_text(text: str) -> list[float]:
    """Generate a vector embedding using the local Nomic model."""

    if not isinstance(text, str) or not text.strip():
        raise ValueError("Text to embed must not be empty.")

    response = requests.post(
        EMBEDDING_URL,
        json={
            "model": EMBEDDING_MODEL,
            "prompt": text
        },
        timeout=TIMEOUT
    )

    response.raise_for_status()

    embedding = response.json().get("embedding")

    if not isinstance(embedding, list):
        raise RuntimeError("Ollama did not return a valid embedding.")

    if len(embedding) != EXPECTED_DIMENSIONS:
        raise RuntimeError(
            f"Expected {EXPECTED_DIMENSIONS} dimensions, "
            f"received {len(embedding)}."
        )

    return embedding


if __name__ == "__main__":
    sample = "Kafka consumer lag caused delayed data processing."
    vector = embed_text(sample)

    print(f"Model: {EMBEDDING_MODEL}")
    print(f"Dimensions: {len(vector)}")
    print(f"Sample values: {vector[:5]}")
