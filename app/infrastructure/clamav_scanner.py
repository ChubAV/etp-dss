import asyncio
import struct
import time

import structlog

from app.domain.scanner import ScanResult

logger = structlog.get_logger()

CHUNK_SIZE = 8192


class ClamAVScanner:
    def __init__(self, host: str, port: int, timeout: float = 30.0) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout

    async def scan(self, data: bytes) -> ScanResult:
        start = time.monotonic()
        reader, writer = await asyncio.open_connection(self._host, self._port)
        try:
            writer.write(b"zINSTREAM\0")
            await writer.drain()
            offset = 0
            while offset < len(data):
                chunk = data[offset : offset + CHUNK_SIZE]
                writer.write(struct.pack(">I", len(chunk)) + chunk)
                offset += len(chunk)
            writer.write(struct.pack(">I", 0))
            await writer.drain()
            response = await asyncio.wait_for(reader.readline(), timeout=self._timeout)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return self._parse_response(response.decode().strip(), elapsed_ms)
        finally:
            writer.close()
            await writer.wait_closed()

    def _parse_response(self, response: str, elapsed_ms: int) -> ScanResult:
        if response.endswith("OK"):
            return ScanResult(
                clean=True,
                engine="ClamAV",
                threats=[],
                signatures_checked=0,
                scan_duration_ms=elapsed_ms,
            )
        threat = response.replace("stream: ", "").replace(" FOUND", "")
        return ScanResult(
            clean=False,
            engine="ClamAV",
            threats=[threat],
            signatures_checked=0,
            scan_duration_ms=elapsed_ms,
        )
