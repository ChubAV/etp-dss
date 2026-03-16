from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v2.routes_health import router as health_router
from app.config import Settings
from app.infrastructure.correlation import CorrelationMiddleware
from app.infrastructure.logging import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    settings = Settings()
    app.state.settings = settings

    engine = create_async_engine(settings.database_url, echo=False)
    app.state.db_session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)

    yield

    await app.state.redis.aclose()
    await engine.dispose()


app = FastAPI(title="Document Storage Service", version="0.1.0", lifespan=lifespan)
app.add_middleware(CorrelationMiddleware)
app.include_router(health_router)
