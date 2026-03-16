import hashlib
from collections.abc import AsyncIterator

import gostcrypto


async def compute_hashes(chunks: AsyncIterator[bytes]) -> tuple[str, str]:
    """Compute SHA-256 and GOST 34.11-2012-256 in a single streaming pass."""
    sha256 = hashlib.sha256()
    gost = gostcrypto.gosthash.new('streebog256')

    async for chunk in chunks:
        if chunk:
            sha256.update(chunk)
            gost.update(chunk)

    return sha256.hexdigest(), gost.hexdigest()
