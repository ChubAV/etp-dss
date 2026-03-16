import asyncio
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from prometheus_client import make_asgi_app
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v2.exception_handlers import register_exception_handlers
from app.api.v2.routes_download import router as download_router
from app.api.v2.routes_download_token import router as download_token_router
from app.api.v2.routes_health import router as health_router
from app.api.v2.routes_metadata import router as metadata_router
from app.api.v2.routes_public import router as public_router
from app.api.v2.routes_upload import router as upload_router
from app.api.v2.routes_upload_token import router as upload_token_router
from app.config import Settings
from app.domain.download_token_service import DownloadTokenService
from app.domain.quarantine_service import QuarantineService
from app.domain.upload_token_service import UploadTokenService
from app.infrastructure.av_consumer import AVResultConsumer
from app.infrastructure.av_task_producer import AVTaskProducer
from app.infrastructure.correlation import CorrelationMiddleware
from app.infrastructure.events import EventProducer
from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.logging import setup_logging
from app.storage.cache_client import CacheClient
from app.storage.metadata_repository import MetadataRepository
from app.storage.s3_client import S3Client


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    logger = structlog.get_logger()
    settings = Settings()
    app.state.settings = settings

    engine = create_async_engine(settings.database_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.db_session_factory = session_factory
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.s3 = S3Client(
        endpoint_url=settings.s3_endpoint,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        region=settings.s3_region,
    )

    cache = CacheClient(app.state.redis)
    app.state.cache = cache

    app.state.upload_token_service = UploadTokenService(
        secret=settings.upload_token_secret,
        algorithm=settings.upload_token_algorithm,
        ttl_seconds=settings.upload_token_ttl_seconds,
        cache=cache,
    )
    app.state.download_token_service = DownloadTokenService(
        secret=settings.download_token_secret,
        algorithm=settings.download_token_algorithm,
        max_ttl_seconds=settings.download_token_max_ttl_seconds,
        cache=cache,
    )

    # Kafka
    kafka_producer = None
    try:
        kafka_producer = KafkaProducer(settings.kafka_bootstrap)
        await kafka_producer.start()
    except Exception:
        logger.warning("kafka_unavailable_at_startup")

    app.state.kafka_producer = kafka_producer

    if kafka_producer:
        app.state.av_task_producer = AVTaskProducer(
            kafka=kafka_producer,
            topic=settings.kafka_topic_av_request,
        )
        app.state.event_producer = EventProducer(
            kafka=kafka_producer,
            topic=settings.kafka_topic_events,
        )
    else:
        app.state.av_task_producer = None
        app.state.event_producer = None

    # AV Result Consumer (background task)
    av_consumer_task = None
    if kafka_producer:
        from aiokafka import AIOKafkaConsumer as KafkaConsumer

        kafka_consumer = KafkaConsumer(
            settings.kafka_topic_av_result,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id="dss-av-consumer",
        )
        try:
            await kafka_consumer.start()

            async def _consume_av_results():
                async for msg in kafka_consumer:
                    session = session_factory()
                    try:
                        repo = MetadataRepository(session)
                        quarantine_svc = QuarantineService(
                            s3=app.state.s3,
                            repo=repo,
                            events=app.state.event_producer,
                        )
                        consumer = AVResultConsumer(
                            repo=repo,
                            quarantine_service=quarantine_svc,
                            events=app.state.event_producer,
                        )
                        await consumer.handle_message(msg.value)
                        await session.commit()
                    except Exception:
                        await session.rollback()
                        logger.exception("av_result_processing_error")
                    finally:
                        await session.close()

            av_consumer_task = asyncio.create_task(_consume_av_results())
        except Exception:
            logger.warning("kafka_consumer_unavailable")

    yield

    if av_consumer_task:
        av_consumer_task.cancel()
        try:
            await av_consumer_task
        except asyncio.CancelledError:
            pass
    if kafka_producer:
        await kafka_producer.stop()
    await app.state.redis.aclose()
    await engine.dispose()


app = FastAPI(title="Document Storage Service", version="0.1.0", lifespan=lifespan)
register_exception_handlers(app)
app.add_middleware(CorrelationMiddleware)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.include_router(health_router)
app.include_router(upload_token_router)
app.include_router(upload_router)
app.include_router(download_token_router)
app.include_router(download_router)
app.include_router(public_router)
app.include_router(metadata_router)
