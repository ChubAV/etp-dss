from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

router = APIRouter()


@router.get("/health")
async def liveness():
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(request: Request):
    checks = {}
    all_ok = True

    # PostgreSQL
    try:
        async with request.app.state.db_session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"error: {e}"
        all_ok = False

    # Redis
    try:
        await request.app.state.redis.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"
        all_ok = False

    # S3
    try:
        s3 = request.app.state.s3
        await s3.head_bucket(request.app.state.settings.s3_bucket_private)
        checks["s3"] = "ok"
    except Exception as e:
        checks["s3"] = f"error: {e}"
        all_ok = False

    return JSONResponse(content=checks, status_code=200 if all_ok else 503)
