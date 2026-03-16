from app.domain.scanner import ScanResult


def test_scan_result_clean():
    result = ScanResult(
        clean=True,
        engine="ClamAV 1.3.1",
        threats=[],
        signatures_checked=8742156,
        scan_duration_ms=340,
    )
    assert result.clean is True and result.threats == []


def test_scan_result_infected():
    result = ScanResult(
        clean=False,
        engine="ClamAV 1.3.1",
        threats=["Win.Test.EICAR_HDB-1"],
        signatures_checked=8742156,
        scan_duration_ms=120,
    )
    assert result.clean is False and len(result.threats) == 1
