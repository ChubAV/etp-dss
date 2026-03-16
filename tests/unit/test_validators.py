import pytest
from app.domain.validators import validate_content_type, validate_file_size

ALLOWED = ["application/pdf", "image/png", "image/jpeg"]


def test_validate_content_type_allowed():
    validate_content_type("application/pdf", ALLOWED)


def test_validate_content_type_not_allowed():
    with pytest.raises(ValueError, match="not allowed"):
        validate_content_type("application/zip", ALLOWED)


def test_validate_file_size_within_limit():
    validate_file_size(1024, max_size_mb=20)


def test_validate_file_size_exceeds_limit():
    with pytest.raises(ValueError, match="exceeds"):
        validate_file_size(21 * 1024 * 1024, max_size_mb=20)
