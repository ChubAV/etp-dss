from app.infrastructure.metrics import AV_SCAN_DURATION, AV_SCAN_RESULTS, QUARANTINE_FILES


def test_av_metrics_exist():
    assert AV_SCAN_DURATION is not None
    assert AV_SCAN_RESULTS is not None
    assert QUARANTINE_FILES is not None


def test_av_scan_results_has_status_label():
    AV_SCAN_RESULTS.labels(status="CLEAN").inc()


def test_av_scan_duration_has_labels():
    AV_SCAN_DURATION.labels(status="CLEAN", engine="ClamAV").observe(0.5)
