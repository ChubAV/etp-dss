import structlog
from aiokafka import AIOKafkaProducer

logger = structlog.get_logger()


class KafkaProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None
        self._started = False

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
        await self._producer.start()
        self._started = True
        logger.info("kafka_producer_started")

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._started = False
            logger.info("kafka_producer_stopped")

    async def send(self, topic: str, key: str, value: bytes) -> None:
        if not self._started or self._producer is None:
            raise RuntimeError("KafkaProducer not started")
        await self._producer.send_and_wait(topic, key=key.encode(), value=value)
