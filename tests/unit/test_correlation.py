import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from app.infrastructure.correlation import CorrelationMiddleware

app = FastAPI()
app.add_middleware(CorrelationMiddleware)


@app.get("/test")
async def test_endpoint(request: Request):
    return JSONResponse({"correlation_id": request.state.correlation_id})


async def test_generates_correlation_id_if_missing():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test")
    assert response.status_code == 200
    cid = response.json()["correlation_id"]
    uuid.UUID(cid)
    assert response.headers["x-correlation-id"] == cid


async def test_uses_provided_correlation_id():
    test_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-Correlation-Id": test_id})
    assert response.json()["correlation_id"] == test_id
    assert response.headers["x-correlation-id"] == test_id
