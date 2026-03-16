from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.infrastructure.clamav_scanner import ClamAVScanner


@pytest.fixture
def scanner():
    return ClamAVScanner(host="localhost", port=3310)


async def test_scan_clean_file(scanner):
    mock_reader = AsyncMock()
    mock_reader.readline = AsyncMock(return_value=b"stream: OK\n")
    mock_writer = MagicMock()
    mock_writer.write = MagicMock()
    mock_writer.drain = AsyncMock()
    mock_writer.close = MagicMock()
    mock_writer.wait_closed = AsyncMock()
    with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
        result = await scanner.scan(b"clean file content")
    assert result.clean is True and result.threats == [] and "ClamAV" in result.engine


async def test_scan_infected_file(scanner):
    mock_reader = AsyncMock()
    mock_reader.readline = AsyncMock(return_value=b"stream: Win.Test.EICAR_HDB-1 FOUND\n")
    mock_writer = MagicMock()
    mock_writer.write = MagicMock()
    mock_writer.drain = AsyncMock()
    mock_writer.close = MagicMock()
    mock_writer.wait_closed = AsyncMock()
    with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
        result = await scanner.scan(b"infected content")
    assert result.clean is False and "Win.Test.EICAR_HDB-1" in result.threats


async def test_scan_connection_error(scanner):
    with patch("asyncio.open_connection", side_effect=ConnectionRefusedError("refused")):
        with pytest.raises(ConnectionRefusedError):
            await scanner.scan(b"data")
