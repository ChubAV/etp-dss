import uuid

from app.domain.db_models import File


def test_file_model_has_required_columns():
    required = [
        "id", "original_name", "storage_key", "bucket", "content_type", "size_bytes",
        "checksum_sha256", "checksum_gost", "owner_type", "owner_id", "version",
        "s3_version_id", "is_latest", "previous_version_id", "visibility",
        "av_status", "av_scanned_at", "av_engine", "av_report",
        "uploaded_by", "uploaded_at", "deleted_at", "correlation_id", "metadata_",
    ]
    columns = {c.key for c in File.__table__.columns}
    for col in required:
        assert col in columns, f"Missing column: {col}"


def test_file_model_defaults():
    f = File(
        original_name="test.pdf",
        storage_key="quarantine/test",
        bucket="documents-quarantine",
        content_type="application/pdf",
        size_bytes=1024,
        checksum_sha256="abc",
        checksum_gost="def",
        owner_type="LOT",
        owner_id=uuid.uuid4(),
        uploaded_by=uuid.uuid4(),
    )
    assert f.version == 1
    assert f.is_latest is True
    assert f.visibility == "PRIVATE"
    assert f.av_status == "PENDING"
