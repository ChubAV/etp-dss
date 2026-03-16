import uuid
from datetime import datetime, timedelta, timezone

import jwt

from app.storage.cache_client import CacheClient


class DownloadTokenService:
    def __init__(self, secret: str, algorithm: str, max_ttl_seconds: int, cache: CacheClient):
        self._secret = secret
        self._algorithm = algorithm
        self._max_ttl_seconds = max_ttl_seconds
        self._cache = cache

    def generate(
        self, file_id: uuid.UUID, user_id: uuid.UUID,
        version: int | None, disposition: str, expires_in_seconds: int,
    ) -> str:
        ttl = min(expires_in_seconds, self._max_ttl_seconds)
        now = datetime.now(timezone.utc)
        return jwt.encode(
            {
                "jti": str(uuid.uuid4()), "file_id": str(file_id),
                "user_id": str(user_id), "version": version,
                "disposition": disposition,
                "iat": now, "exp": now + timedelta(seconds=ttl),
            },
            self._secret, algorithm=self._algorithm,
        )

    async def validate(self, token: str) -> dict:
        try:
            payload = jwt.decode(token, self._secret, algorithms=[self._algorithm])
        except jwt.ExpiredSignatureError:
            raise ValueError("Download token has expired")
        except jwt.InvalidTokenError as e:
            raise ValueError(f"Invalid download token: {e}")
        if await self._cache.is_token_blacklisted(payload.get("jti")):
            raise ValueError("Download token has been blacklisted")
        return payload

    async def consume(self, token: str) -> dict:
        payload = await self.validate(token)
        remaining = max(int(payload["exp"] - datetime.now(timezone.utc).timestamp()), 1)
        await self._cache.blacklist_token(payload["jti"], ttl_seconds=remaining)
        return payload
