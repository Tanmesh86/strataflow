from embeddings import embed_text
from vector_store import search_documents


query = (
    "How should I investigate failed records in the DLQ, "
    "and is automatic replay supported?"
)

query_vector = embed_text(query)
results = search_documents(query_vector, top_k=3)

print(f"\nQuery: {query}")
print(f"Results found: {len(results)}")

for index, result in enumerate(results, start=1):
    print(f"\n--- Result {index} ---")
    print(f"Source: {result['metadata']['source']}")
    print(f"Distance: {result['distance']:.4f}")
    print(result["document"])
