# DSS (Document Storage Service)

Микросервис хранения, управления и доставки файлов для электронной торговой площадки (ETP). Единая точка входа для всех файловых операций платформы.

## Возможности

- **Two-phase upload** — бизнес-сервис запрашивает upload-token, клиент загружает файл напрямую в DSS
- **Антивирусная проверка** — файлы проходят через карантин и ClamAV перед доставкой
- **Dual hashing** — SHA-256 + GOST 34.11-2012 (Streebog) в одном streaming-проходе
- **Presigned URL** — скачивание через S3 presigned URL с кэшированием в Redis
- **One-time токены** — upload/download токены с blacklist в Redis (JWT, 10 мин TTL)
- **Lifecycle events** — Kafka-события о статусе файлов (AV passed/failed, moved, deleted)
- **Prometheus метрики** — upload/download/AV scan статистика

## Tech Stack

| Компонент | Технология |
|-----------|-----------|
| Язык | Python 3.12 |
| Фреймворк | FastAPI (async) |
| БД | PostgreSQL 16 (SQLAlchemy 2.0 async + asyncpg) |
| Миграции | Alembic |
| Объектное хранилище | S3-compatible (MinIO / Timeweb S3) |
| Кэш | Redis 7 |
| Очередь | Apache Kafka |
| Антивирус | ClamAV (TCP INSTREAM) |
| ГОСТ-хэширование | gostcrypto (Streebog 34.11-2012) |
| Пакетный менеджер | uv |

## Быстрый старт

```bash
# Установка зависимостей
uv sync

# Копирование конфигурации
cp .env.example .env

# Запуск инфраструктуры (PostgreSQL, Redis, MinIO, Kafka, ClamAV)
docker compose up -d

# Применение миграций
just migrate

# Запуск сервиса
just run
```

Сервис будет доступен на `http://localhost:8001`.

## Команды

```bash
just run              # Запуск с hot-reload
just test             # Все тесты
just test-unit        # Unit-тесты
just test-integration # Интеграционные тесты
just lint             # Проверка ruff
just fix              # Автоисправление ruff
just migration "msg"  # Создание миграции Alembic
just migrate          # Применение миграций
just up               # docker compose up
just down             # docker compose down
just rebuild          # Пересборка контейнеров
```

## Архитектура

```
API Layer        /api/v2/documents
  routes_upload_token.py    POST /upload-token
  routes_upload.py          POST /upload
  routes_download_token.py  POST /download-token
  routes_download.py        GET  /download, POST /{id}/presigned-url
  routes_public.py          GET  /public/{id}/download
  routes_metadata.py        GET  /{id}, GET /by-owner/{type}/{id}, DELETE /{id}
  routes_health.py          GET  /health/live, /health/ready

Domain Layer
  file_service.py           Upload, download, metadata CRUD
  quarantine_service.py     Promote: copy -> verify -> delete -> events
  upload_token_service.py   JWT генерация/валидация/consume
  download_token_service.py JWT генерация/валидация/consume

Storage Layer
  s3_client.py              aiobotocore (upload, download, copy, delete)
  metadata_repository.py    SQLAlchemy async CRUD
  cache_client.py           Redis (token blacklist, presigned URL cache)

Infrastructure
  kafka_producer.py         Общий aiokafka producer
  av_task_producer.py       Публикация AV scan request
  av_consumer.py            Обработка AV scan result
  events.py                 Lifecycle events (FILE_AV_PASSED, etc.)
  clamav_scanner.py         TCP INSTREAM клиент ClamAV

Standalone
  av_worker.py              Отдельный процесс: Kafka -> ClamAV -> Kafka
```

## Поток загрузки файла

```
Client                    DSS API                  S3 Quarantine       Kafka         AV Worker        ClamAV
  |                         |                          |                 |              |               |
  |-- POST /upload-token -->|                          |                 |              |               |
  |<-- token ---------------|                          |                 |              |               |
  |                         |                          |                 |              |               |
  |-- POST /upload -------->|                          |                 |              |               |
  |                         |-- PUT object ----------->|                 |              |               |
  |                         |-- av.scan.request ----------------------->|              |               |
  |<-- 201 file_id ---------|                          |                 |              |               |
  |                         |                          |                 |-- consume -->|               |
  |                         |                          |<-- GET object --|              |               |
  |                         |                          |                 |              |-- INSTREAM -->|
  |                         |                          |                 |              |<-- OK --------|
  |                         |                          |                 |<-- result ---|               |
  |                         |<-- av.scan.result -------|-----------------|              |               |
  |                         |-- COPY to target ------->|                 |              |               |
  |                         |-- DELETE quarantine ----->|                 |              |               |
```

## S3 Bucket Layout

| Bucket | Назначение | Шифрование | Lifecycle |
|--------|-----------|-----------|-----------|
| `documents-private` | Приватные файлы (заявки, протоколы) | SSE-S3 | Версионирование |
| `documents-public` | Публичные документы извещений | SSE-S3 | — |
| `documents-quarantine` | Временное хранение до AV scan | SSE-S3 | Auto-delete 24h |

## Аутентификация

| Метод | Заголовок | Кто использует |
|-------|----------|---------------|
| Upload-token | `X-Upload-Token` | Web-клиент (загрузка) |
| Download-token | `?token=` query param | Web-клиент (скачивание) |
| Service JWT | `Authorization: Bearer` | M2M (бизнес-сервисы) |
| Без авторизации | — | Публичные файлы (`/public/{id}/download`) |

## Тесты

77 unit-тестов покрывают:
- Все доменные сервисы (upload, download, quarantine promotion)
- Token generation/validation/consume
- S3 client operations
- Kafka producer/consumer
- ClamAV scanner (TCP protocol)
- Download guards (AV status check)
- Prometheus metrics
- Exception handlers

```bash
just test-unit  # ~2 секунды
```

## Документация

- `docs/tz_dss.md` — полное техническое задание (на русском)
- `docs/superpowers/specs/` — дизайн-спецификации
- `docs/superpowers/plans/` — планы реализации
