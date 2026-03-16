# DSS (Document Storage Service) — Дизайн реализации

**Дата:** 2026-03-16
**Источник:** docs/tz_dss.md (архитектурный проект v2.3)

---

## 1. Подход к реализации

Реализация разбита на 5 вертикальных срезов. Каждый срез даёт рабочий end-to-end сценарий от API до инфраструктуры. Срезы реализуются последовательно — каждый следующий расширяет предыдущий.

**Отклонение от ТЗ:** в ТЗ (секция 12) domain-слой содержит один `models.py` для Pydantic-моделей. В реализации мы разделяем на `domain/schemas.py` (Pydantic request/response) и `domain/db_models.py` (SQLAlchemy ORM) — разная ответственность, файл `models.py` стал бы слишком большим.

---

## 2. Общая архитектура

### 2.1. Структура проекта

Берётся из ТЗ (секция 12):

```
app/
├── main.py                    # FastAPI app, lifespan, exception handlers
├── config.py                  # Settings(BaseSettings) из pydantic-settings
├── dependencies.py            # DI через FastAPI Depends
├── api/v2/                    # Роутеры (по одному на функциональную область)
├── domain/                    # Бизнес-логика, сервисы, валидаторы
│   ├── schemas.py             # Pydantic-схемы (request/response)
│   └── db_models.py           # SQLAlchemy ORM-модели
├── storage/                   # S3, PostgreSQL repository, Redis
├── infrastructure/            # Auth, Kafka, Crypto EDS client, metrics, logging
└── workers/                   # ClamAV worker (отдельный процесс)

tests/
├── unit/                      # Unit-тесты domain-слоя
├── integration/               # Integration-тесты с реальными сервисами
└── fixtures/                  # Фикстуры и фабрики тестовых данных
```

### 2.2. Dependency Injection

Встроенные `Depends` FastAPI. Фабрики клиентов (S3, Redis, DB session) создаются при старте через lifespan, хранятся в `app.state`. Dependencies в routes достают их оттуда.

### 2.3. Конфигурация

Один класс `Settings(BaseSettings)` из pydantic-settings, загружает из `.env`. Все переменные из секции 16.2 ТЗ. Вложенные группы через поля основного Settings (не отдельные классы). Переменная `ACCESS_CONTROL_URL` из ТЗ не используется — DSS не ходит в IAM напрямую (секция 8.1 ТЗ).

### 2.4. База данных

SQLAlchemy 2.0 async, `DeclarativeBase`. `create_async_engine` + `async_sessionmaker`. Alembic для миграций.

### 2.5. Обработка ошибок

Кастомные exception-классы (`FileNotFoundError`, `TokenExpiredError`, `AVNotPassedError`, etc.), FastAPI exception handlers маппят их на HTTP-коды.

### 2.6. Тестирование

pytest + pytest-asyncio. Unit-тесты для domain-слоя. Integration-тесты с реальными PostgreSQL, MinIO, Redis через docker compose.

### 2.7. Retry-политики (сквозные)

Все retry-параметры из ТЗ (секция 10.2, 14):
- S3 операции: retry 3x с exponential backoff
- Kafka: retry с exponential backoff
- AV-проверка: 3 попытки (5/15/30 сек)
- Верификация подписей: 3 попытки (3/10/30 сек)
- Crypto EDS HTTP: 3 попытки

### 2.8. Redis fail-closed

При недоступности Redis blacklist — токены отклоняются (fail-closed), а не принимаются. Это требование безопасности из ТЗ (секция 14).

---

## 3. Срез 1: Каркас + Upload + Download

Самый объёмный срез — включает весь каркас приложения.

### 3.1. Результат

После реализации: можно загружать и скачивать файлы через полный цикл upload-token → upload → download-token → download. Публичные файлы доступны без авторизации. Health/readiness проверки работают.

### 3.2. Компоненты

**Точка входа и конфигурация:**
- `app/main.py` — FastAPI app, lifespan (инициализация S3/Redis/DB), подключение роутеров, exception handlers
- `app/config.py` — `Settings(BaseSettings)` со всеми переменными из ТЗ секция 16.2
- `app/dependencies.py` — `get_db_session()`, `get_s3_client()`, `get_redis()`, `get_file_service()` и т.д.

**Storage layer:**
- `storage/s3_client.py` — async обёртка над aiobotocore: `upload_object()`, `generate_presigned_url()`, `copy_object()`, `delete_object()`
- `storage/metadata_repository.py` — CRUD для таблицы `files` через SQLAlchemy async. Все запросы фильтруют `WHERE deleted_at IS NULL` (soft delete). Пагинация для `by-owner`: параметры `page`, `page_size` (default 20, max 100), фильтр `av_status`, флаг `include_signatures`.
- `storage/cache_client.py` — Redis: кеш presigned URL, blacklist токенов (jti)

**Domain layer:**
- `domain/schemas.py` — Pydantic-схемы запросов/ответов
- `domain/db_models.py` — SQLAlchemy ORM-модель `File`
- `domain/file_service.py` — оркестрация загрузки (валидация → хеширование → S3 → метаданные → blacklist jti) и скачивания
- `domain/hash_calculator.py` — streaming SHA-256 + ГОСТ 34.11-2012 через pygost в одном проходе
- `domain/upload_token_service.py` — генерация/валидация JWT upload-token. При генерации `uploaded_by` (UUID пользователя) встраивается в JWT payload — при загрузке файла извлекается из токена и записывается в `files.uploaded_by`.
- `domain/download_token_service.py` — генерация/валидация JWT download-token
- `domain/validators.py` — проверка content-type (whitelist: PDF, DOCX, XLSX, JPG, PNG), размера файла

**API layer:**
- `api/v2/routes_upload_token.py` — `POST /upload-token` (M2M, service JWT)
- `api/v2/routes_upload.py` — `POST /upload` (upload-token или service JWT)
- `api/v2/routes_download_token.py` — `POST /download-token` (M2M, service JWT)
- `api/v2/routes_download.py` — `GET /download?token=` (redirect на presigned URL), `POST /{fileId}/presigned-url` (M2M)
- `api/v2/routes_public.py` — `GET /public/{fileId}/download` (без авторизации)
- `api/v2/routes_metadata.py` — `GET /{fileId}`, `GET /by-owner/{ownerType}/{ownerId}` (с пагинацией и фильтрами), `DELETE /{fileId}` (soft delete)
- `api/v2/routes_health.py` — `GET /health` (liveness), `GET /health/ready` (проверка S3 + PostgreSQL + Redis), `GET /metrics` (Prometheus)

**Infrastructure:**
- `infrastructure/auth.py` — dependency для валидации service JWT (scope-based) и upload/download токенов
- `infrastructure/correlation.py` — middleware для `X-Correlation-Id`
- `infrastructure/logging.py` — structlog JSON config
- `infrastructure/metrics.py` — Prometheus counters/histograms из ТЗ секция 11

**Dev-окружение:**
- `docker-compose.yml` — postgres, redis, minio (без kafka/clamav — они для срезов 2-3)
- `alembic.ini` + `migrations/` — начальная миграция с таблицей `files`
- `.env.example`
- `Dockerfile`
- `justfile` — команды для разработки: миграции, запуск, тесты

### 3.3. Ключевые решения

- Файлы в этом срезе загружаются сразу в целевой бакет (private/public), а не в карантин — карантинная логика появляется в срезе 2. `av_status` устанавливается в `PENDING`, но не блокирует скачивание до реализации AV.
- Upload-token и download-token — одноразовые JWT, jti в Redis blacklist. При недоступности Redis — fail-closed (токены отклоняются).
- Streaming хеширование — файл не загружается в память целиком, SHA-256 и ГОСТ вычисляются в одном проходе по чанкам.
- `uploaded_by` извлекается из upload-token (для web-клиентов) или из service JWT claims (для M2M).

---

## 4. Срез 2: Антивирусная проверка

### 4.1. Результат

После реализации: файлы проходят антивирусную проверку перед доступом на скачивание. Файлы с `av_status != CLEAN` недоступны.

### 4.2. Компоненты

- `infrastructure/av_task_producer.py` — публикация задачи в `documents.av.scan.request` при загрузке
- `infrastructure/av_consumer.py` — consumer `documents.av.scan.result`: обновляет `av_status`, копирует из карантина в целевой бакет (с проверкой checksum после копирования), удаляет из карантина
- `workers/av_worker.py` — отдельный процесс: consumer `av.scan.request` → clamd socket → publish `av.scan.result`. Retry 3x (5/15/30 сек) при сбое ClamAV.
- `domain/av_service.py` — оркестрация AV-потока
- `infrastructure/events.py` — Kafka producer для `documents.events`. В этом срезе эмитит: `FILE_UPLOADED`, `FILE_AV_PASSED`, `FILE_AV_FAILED`, `FILE_MOVED_TO_STORAGE`.

### 4.3. Изменения в срезе 1

- Загрузка теперь идёт в `documents-quarantine`, а не в целевой бакет.
- Скачивание проверяет `av_status = CLEAN` — файлы в карантине недоступны.
- Docker Compose: добавляются `kafka`, `clamav`, `av-worker`.

---

## 5. Срез 3: Электронные подписи

### 5.1. Результат

После реализации: можно привязывать ЭЦП к файлам и проверять их через Crypto EDS (синхронно по HTTP или асинхронно через Kafka).

### 5.2. Компоненты

- `storage/signature_repository.py` — CRUD для `file_signatures`
- `domain/signature_service.py` — sync путь: читает `checksum_gost` из `files`, вызывает Crypto EDS по HTTP, сохраняет результат (включая невалидные подписи — для аудита). Retry 3x (3/10/30 сек).
- `infrastructure/crypto_eds_client.py` — aiohttp клиент к Crypto EDS (`POST /api/v1/verify`)
- `infrastructure/signature_consumer.py` — consumer `documents.signatures.request` для async пути. Двухэтапный Kafka-поток: (1) consume из `documents.signatures.request` → сохранить с `PENDING` → publish в `tasks.crypto_eds`, (2) consume из `results.crypto_eds` → обновить `file_signatures` → publish `SIGNATURE_VERIFIED` в `documents.events`.
- `api/v2/routes_signatures.py` — `POST /{fileId}/signatures` (sync), `GET /{fileId}/signatures`, `POST /{fileId}/signatures/{signatureId}/reverify` (всегда async, 202 Accepted)
- Events: `SIGNATURE_ADDED`, `SIGNATURE_VERIFIED`, `SIGNATURE_REVERIFIED`

### 5.3. Миграция

Alembic: таблица `file_signatures` (структура из ТЗ секция 4.2).

---

## 6. Срез 4: Версионирование

### 6.1. Результат

После реализации: полная поддержка версий файлов — загрузка новой версии, список версий, навигация по цепочке.

### 6.2. Компоненты

- `domain/version_service.py` — создание новой версии (новая запись в `files`, `is_latest` flip, наследование `owner_type`/`owner_id`)
- `api/v2/routes_versions.py` — `POST /{fileId}/versions`, `GET /{fileId}/versions`
- Логика обхода цепочки `previous_version_id` в `metadata_repository.py`
- Event: `FILE_VERSION_CREATED`

### 6.3. Ключевые решения

- Подписи не наследуются — новая версия создаётся с пустым `signatures[]`.
- `is_latest = false` для старой версии устанавливается в одной транзакции с INSERT новой.
- Optimistic locking: проверка `is_latest = true` в WHERE при UPDATE.

---

## 7. Срез 5: Rate limiting + аудит + CDN

### 7.1. Результат

После реализации: production-ready сервис с rate limiting, аудит-логами и CDN-кешированием.

### 7.2. Компоненты

- `infrastructure/rate_limiter.py` — Redis sliding window, FastAPI dependency для routes. Лимиты из ТЗ секция 8.3:
  - Upload: 30 файлов / 5 мин (на пользователя)
  - Download presigned URL: 100 / мин (на пользователя)
  - Публичные документы: 200 / мин (на IP)
  - API метаданных: 1000 / мин (на сервис)
- `infrastructure/audit.py` — Kafka producer в `audit.document_storage`. Все 13 типов событий из ТЗ секция 7.4.
- CDN-заголовки в `routes_public.py`: `Cache-Control: public, max-age=3600`, `ETag`, `If-None-Match` → `304 Not Modified`. Инвалидация кеша при обновлении версии публичного файла (через новый URL — file_id меняется при создании версии).
- Оставшиеся events: `FILE_DELETED`, `FILE_DOWNLOADED`, `PRESIGNED_URL_GENERATED`

### 7.3. Reconciliation job

Ежедневная задача сверки PostgreSQL метаданных с S3 объектами (ТЗ секция 15, R-06). Проверяет наличие объектов в S3 и валидность checksum. Реализуется как management-команда или отдельный worker.

---

## 8. Зависимости (pyproject.toml)

Основные пакеты:
- `fastapi`, `uvicorn[standard]`
- `sqlalchemy[asyncio]`, `asyncpg`, `alembic`
- `aiobotocore`
- `redis[hiredis]` (async)
- `pyjwt`
- `pygost`
- `structlog`
- `prometheus-client`
- `pydantic-settings`
- `aiohttp` (HTTP-клиент для Crypto EDS)
- `aiokafka`
- `python-multipart` (для file uploads в FastAPI)

Dev:
- `pytest`, `pytest-asyncio`
- `httpx` (для TestClient)
- `ruff` (linter/formatter)
