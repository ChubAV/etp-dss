from datetime import datetime, timedelta, timezone
import jwt
import pytest
from app.infrastructure.auth import decode_service_jwt, verify_scope

SECRET = "test-service-jwt-secret-key"


def _make_token(scopes, exp_delta=600):
    return jwt.encode(
        {"sub": "notice-service", "scopes": scopes,
         "iat": datetime.now(timezone.utc),
         "exp": datetime.now(timezone.utc) + timedelta(seconds=exp_delta)},
        SECRET, algorithm="HS256",
    )


def test_decode_valid():
    payload = decode_service_jwt(_make_token(["documents.read"]), SECRET, "HS256")
    assert payload["sub"] == "notice-service"


def test_decode_expired():
    with pytest.raises(ValueError, match="expired"):
        decode_service_jwt(_make_token(["documents.read"], -10), SECRET, "HS256")


def test_verify_scope_passes():
    verify_scope({"scopes": ["documents.read", "documents.write"]}, "documents.read")


def test_verify_scope_fails():
    with pytest.raises(PermissionError, match="scope"):
        verify_scope({"scopes": ["documents.read"]}, "documents.write")
