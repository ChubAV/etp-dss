import hashlib
from app.domain.hash_calculator import compute_hashes


async def test_compute_hashes_returns_sha256_and_gost():
    data = b"Hello, Document Storage Service!"

    async def chunk_iter():
        yield data

    sha256_hex, gost_hex = await compute_hashes(chunk_iter())
    assert sha256_hex == hashlib.sha256(data).hexdigest()
    assert len(gost_hex) == 64  # GOST 256-bit = 64 hex chars


async def test_compute_hashes_streaming_multiple_chunks():
    chunks = [b"chunk1", b"chunk2", b"chunk3"]
    full_data = b"".join(chunks)

    async def chunk_iter():
        for c in chunks:
            yield c

    sha256_hex, gost_hex = await compute_hashes(chunk_iter())
    assert sha256_hex == hashlib.sha256(full_data).hexdigest()
    assert len(gost_hex) == 64
