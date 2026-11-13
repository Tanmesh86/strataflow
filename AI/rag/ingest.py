
import hashlib
import re
from pathlib import Path

try:
    from .embeddings import embed_text
    from .vector_store import add_documents
except ImportError:
    from embeddings import embed_text
    from vector_store import add_documents


KNOWLEDGE_DIR = Path(__file__).resolve().parent / "knowledge"
CHUNK_SIZE = 1200
CHUNK_OVERLAP = 0


def split_oversized_paragraph(paragraph: str) -> list[str]:
    """Split oversized text at sentence boundaries where possible."""

    if len(paragraph) <= CHUNK_SIZE:
        return [paragraph]

    sentences = re.split(r"(?<=[.!?])\s+", paragraph)

    parts = []
    current = ""

    for sentence in sentences:
        if len(sentence) > CHUNK_SIZE:
            if current:
                parts.append(current)
                current = ""

            remaining = sentence

            while len(remaining) > CHUNK_SIZE:
                boundary = remaining.rfind(" ", 0, CHUNK_SIZE)

                if boundary <= 0:
                    boundary = CHUNK_SIZE

                parts.append(remaining[:boundary].strip())
                remaining = remaining[boundary:].strip()

            current = remaining
            continue

        candidate = f"{current} {sentence}".strip() if current else sentence

        if len(candidate) <= CHUNK_SIZE:
            current = candidate
        else:
            if current:
                parts.append(current)

            current = sentence

    if current:
        parts.append(current)

    return parts


def split_markdown_blocks(text: str) -> list[str]:
    """Split Markdown into blocks while preserving fenced code blocks."""

    lines = text.splitlines()
    blocks = []
    current = []
    inside_fence = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("```"):
            inside_fence = not inside_fence
            current.append(line)
            continue

        if not stripped and not inside_fence:
            if current:
                blocks.append("\n".join(current).strip())
                current = []
            continue

        current.append(line)

    if current:
        blocks.append("\n".join(current).strip())

    if inside_fence:
        raise ValueError("Unclosed Markdown code fence detected.")

    return [block for block in blocks if block]


def chunk_text(text: str) -> list[str]:
    """Create Markdown-aware chunks without overlap."""

    text = text.strip()

    if not text:
        return []

    blocks = split_markdown_blocks(text)

    # Keep each heading with its following content block.
    grouped_blocks = []
    index = 0

    while index < len(blocks):
        block = blocks[index]

        if block.startswith("#") and index + 1 < len(blocks):
            block += "\n\n" + blocks[index + 1]
            index += 1

        grouped_blocks.extend(split_oversized_paragraph(block))
        index += 1

    chunks = []
    current_parts = []
    current_length = 0

    for block in grouped_blocks:
        required_length = len(block) + (
            2 if current_parts else 0
        )

        if (
            current_parts
            and current_length + required_length > CHUNK_SIZE
        ):
            chunks.append("\n\n".join(current_parts))
            current_parts = []
            current_length = 0

        current_parts.append(block)
        current_length += len(block) + 2

    if current_parts:
        chunks.append("\n\n".join(current_parts))

    return chunks


def ingest_knowledge():
    files = sorted(KNOWLEDGE_DIR.rglob("*.md"))

    if not files:
        raise FileNotFoundError(
            f"No Markdown knowledge files found in {KNOWLEDGE_DIR}"
        )

    all_ids = []
    all_documents = []
    all_embeddings = []
    all_metadatas = []

    for file_path in files:
        content = file_path.read_text(encoding="utf-8-sig")
        chunks = chunk_text(content)

        print(f"\nProcessing: {file_path.name}")
        print(f"Chunks: {len(chunks)}")

        for index, chunk in enumerate(chunks):
            embedding = embed_text(chunk)

            chunk_id = hashlib.sha256(
                f"{file_path.as_posix()}:{index}:{chunk}".encode("utf-8")
            ).hexdigest()

            all_ids.append(chunk_id)
            all_documents.append(chunk)
            all_embeddings.append(embedding)
            all_metadatas.append({
                "source": file_path.name,
                "doc_type": "knowledge",
                "chunk_index": index
            })

    add_documents(
        ids=all_ids,
        documents=all_documents,
        embeddings=all_embeddings,
        metadatas=all_metadatas
    )

    print("\nIngestion completed.")
    print(f"Files processed: {len(files)}")
    print(f"Chunks indexed: {len(all_ids)}")


if __name__ == "__main__":
    ingest_knowledge()
