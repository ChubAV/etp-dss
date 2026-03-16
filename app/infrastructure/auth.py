import jwt as pyjwt
from fastapi import Depends, Header, HTTPException, Request


def decode_service_jwt(token: str, secret: str, algorithm: str) -> dict:
    try:
        return pyjwt.decode(token, secret, algorithms=[algorithm])
    except pyjwt.ExpiredSignatureError:
        raise ValueError("Service JWT has expired")
    except pyjwt.InvalidTokenError as e:
        raise ValueError(f"Invalid service JWT: {e}")


def verify_scope(payload: dict, required_scope: str) -> None:
    if required_scope not in payload.get("scopes", []):
        raise PermissionError(f"Missing required scope: {required_scope}")


async def require_service_jwt(
    request: Request,
    authorization: str = Header(...),
) -> dict:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    settings = request.app.state.settings
    try:
        return decode_service_jwt(
            authorization[7:], settings.service_jwt_secret, settings.service_jwt_algorithm,
        )
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))


def require_scope(scope: str):
    async def checker(payload: dict = Depends(require_service_jwt)):
        try:
            verify_scope(payload, scope)
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=str(e))
        return payload
    return checker
