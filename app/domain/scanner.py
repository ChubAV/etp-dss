from typing import Protocol

from pydantic import BaseModel


class ScanResult(BaseModel):
    clean: bool
    engine: str
    threats: list[str]
    signatures_checked: int
    scan_duration_ms: int


class Scanner(Protocol):
    async def scan(self, data: bytes) -> ScanResult: ...
