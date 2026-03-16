import uuid
from datetime import UTC, datetime, timedelta

import jwt

from app.storage.cache_client import CacheClient


class UploadTokenService:
    def __init__(self, secret: str, algorithm: str, ttl_seconds: int, cache: CacheClient):
        self._secret = secret
        self._algorithm = algorithm
        self._ttl_seconds = ttl_seconds
        self._cache = cache

    def generate(
        self,
        owner_type: str,
        owner_id: uuid.UUID,
        visibility: str,
        file_name: str,
        content_type: str,
        max_size_bytes: int,
        uploaded_by: uuid.UUID,
    ) -> str:
        now = datetime.now(UTC)
        return jwt.encode(
            {
                "jti": str(uuid.uuid4()),
                "owner_type": owner_type,
                "owner_id": str(owner_id),
                "visibility": visibility,
                "file_name": file_name,
                "content_type": content_type,
                "max_size_bytes": max_size_bytes,
                "uploaded_by": str(uploaded_by),
                "iat": now,
                "exp": now + timedelta(seconds=self._ttl_seconds),
            },
            self._secret,
            algorithm=self._algorithm,
        )

    async def validate(self, token: str) -> dict:
        try:
            payload = jwt.decode(token, self._secret, algorithms=[self._algorithm])
        except jwt.ExpiredSignatureError:
            raise ValueError("Upload token has expired")
        except jwt.InvalidTokenError as e:
            raise ValueError(f"Invalid upload token: {e}")
        if await self._cache.is_token_blacklisted(payload.get("jti")):
            raise ValueError("Upload token has been blacklisted")
        return payload

    async def consume(self, token: str) -> dict:
        payload = await self.validate(token)
        remaining = max(int(payload["exp"] - datetime.now(UTC).timestamp()), 1)
        await self._cache.blacklist_token(payload["jti"], ttl_seconds=remaining)
        return payload
