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

---

## Руководство по интеграции

Этот раздел предназначен для разработчиков сервисов ETP (Notice Service, Application Service, DocGen Service и др.), которые интегрируются с DSS.

### Базовый URL

```
/api/v2/documents
```

### Аутентификация

DSS поддерживает 4 метода аутентификации в зависимости от сценария:

| Метод | Заголовок/параметр | Кто использует | Scope |
|-------|-------------------|---------------|-------|
| Upload-token | `X-Upload-Token` | Web-клиент (загрузка) | — |
| Download-token | `?token=` query param | Web-клиент (скачивание) | — |
| Service JWT | `Authorization: Bearer <jwt>` | M2M (бизнес-сервисы) | см. ниже |
| Без авторизации | — | Публичные файлы | — |

> DSS **не** выполняет бизнес-авторизацию. Вызывающий сервис сам решает, имеет ли пользователь право на операцию.

#### Формат Service JWT

Все M2M-вызовы требуют JWT в заголовке `Authorization: Bearer <token>`.

**Payload JWT:**

```json
{
  "sub": "service-id-uuid",
  "scopes": ["documents.issue_token", "documents.read", "documents.write", "documents.delete"],
  "iat": 1711000000,
  "exp": 1711003600
}
```

**Доступные scopes:**

| Scope | Назначение |
|-------|-----------|
| `documents.issue_token` | Выпуск upload-token и download-token |
| `documents.read` | Чтение метаданных, получение presigned URL |
| `documents.write` | Прямая загрузка файлов (M2M, без upload-token) |
| `documents.delete` | Удаление файлов |

**Подпись:** HS256, секрет задаётся через `SERVICE_JWT_SECRET` (должен совпадать у DSS и вызывающего сервиса).

---

### Сценарий 1: Загрузка файла через Web-клиент (Two-Phase Upload)

Это основной сценарий. Бизнес-сервис **не проксирует** байты файла — клиент загружает напрямую в DSS.

```
Web-клиент          Бизнес-сервис              DSS
    │                     │                      │
    │── (1) запрос ──────>│                      │
    │                     │── (2) POST           │
    │                     │   /upload-token ────>│
    │                     │<── upload_token ─────│
    │<── upload_token ────│                      │
    │                     │                      │
    │── (3) POST /upload + файл + token ───────>│
    │<── 201 { file_id } ──────────────────────│
    │                     │                      │
    │── (4) confirm ─────>│                      │
    │   file_id           │── (5) GET /{id} ───>│  (опционально)
    │                     │<── metadata ────────│
```

#### Шаг 1: Бизнес-сервис запрашивает upload-token

```bash
curl -X POST https://dss.example.com/api/v2/documents/upload-token \
  -H "Authorization: Bearer <service-jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "owner_type": "APPLICATION",
    "owner_id": "550e8400-e29b-41d4-a716-446655440000",
    "visibility": "PRIVATE",
    "file_name": "charter.pdf",
    "content_type": "application/pdf",
    "max_size_bytes": 20971520,
    "uploaded_by": "660e8400-e29b-41d4-a716-446655440001"
  }'
```

**Требуемый scope:** `documents.issue_token`

**Ответ (201):**

```json
{
  "upload_token": "eyJhbGciOiJIUzI1NiJ9...",
  "expires_at": "2025-01-15T12:10:00Z",
  "owner_type": "APPLICATION",
  "owner_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

> Upload-token действует 10 минут и **одноразовый** — после использования `jti` добавляется в Redis blacklist.

**owner_type** — тип бизнес-сущности, к которой привязывается файл:

| owner_type | Описание |
|------------|---------|
| `APPLICATION` | Заявка участника |
| `LOT` | Лот |
| `NOTICE` | Извещение |
| `DECISION` | Решение |
| `REFUND` | Возврат средств |
| `PROTOCOL` | Протокол |

#### Шаг 2: Web-клиент загружает файл

```bash
curl -X POST https://dss.example.com/api/v2/documents/upload \
  -H "X-Upload-Token: eyJhbGciOiJIUzI1NiJ9..." \
  -F "file=@charter.pdf"
```

**Ответ (201):**

```json
{
  "file_id": "770e8400-e29b-41d4-a716-446655440002",
  "original_name": "charter.pdf",
  "content_type": "application/pdf",
  "size_bytes": 1048576,
  "checksum_sha256": "a1b2c3d4e5f6...",
  "checksum_gost": "f6e5d4c3b2a1...",
  "av_status": "SCANNING",
  "version": 1,
  "visibility": "PRIVATE",
  "owner_type": "APPLICATION",
  "owner_id": "550e8400-e29b-41d4-a716-446655440000",
  "uploaded_by": "660e8400-e29b-41d4-a716-446655440001",
  "uploaded_at": "2025-01-15T12:01:30Z"
}
```

> Обратите внимание: `av_status` = `"SCANNING"`. Файл находится в карантине и **недоступен для скачивания**, пока AV-проверка не завершится со статусом `CLEAN`.

#### Шаг 3: Клиент сообщает file_id бизнес-сервису

Клиент отправляет `file_id` обратно бизнес-сервису, который сохраняет его в своей модели данных.

---

### Сценарий 2: Прямая загрузка (M2M)

Для серверных сервисов (DocGen Service и др.), которые загружают файлы программно.

```bash
curl -X POST "https://dss.example.com/api/v2/documents/upload?owner_type=PROTOCOL&owner_id=550e8400-e29b-41d4-a716-446655440000&visibility=PRIVATE" \
  -H "Authorization: Bearer <service-jwt>" \
  -F "file=@protocol.pdf"
```

**Требуемый scope:** `documents.write`

Query-параметры при M2M-загрузке:

| Параметр | Обязательный | Описание |
|----------|-------------|---------|
| `owner_type` | Да | Тип бизнес-сущности |
| `owner_id` | Да | UUID бизнес-сущности |
| `visibility` | Нет | `PRIVATE` (по умолчанию) или `PUBLIC` |

---

### Сценарий 3: Скачивание файла через Web-клиент

#### Шаг 1: Бизнес-сервис запрашивает download-token

```bash
curl -X POST https://dss.example.com/api/v2/documents/download-token \
  -H "Authorization: Bearer <service-jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "file_id": "770e8400-e29b-41d4-a716-446655440002",
    "user_id": "660e8400-e29b-41d4-a716-446655440001",
    "disposition": "attachment",
    "expires_in_seconds": 300
  }'
```

**Требуемый scope:** `documents.issue_token`

**Ответ (201):**

```json
{
  "download_token": "eyJhbGciOiJIUzI1NiJ9...",
  "expires_at": "2025-01-15T12:06:00Z",
  "file_id": "770e8400-e29b-41d4-a716-446655440002",
  "original_name": "charter.pdf",
  "content_type": "application/pdf",
  "size_bytes": 1048576
}
```

> Если файл не прошёл AV-проверку, запрос вернёт **403 Forbidden**.

#### Шаг 2: Web-клиент скачивает файл

```
GET /api/v2/documents/download?token=eyJhbGciOiJIUzI1NiJ9...
```

DSS отвечает **302 Redirect** на presigned S3 URL. Download-token одноразовый.

---

### Сценарий 4: Получение presigned URL (M2M)

Для сервисов, которым нужна прямая ссылка на файл (Crypto EDS и др.).

```bash
curl -X POST https://dss.example.com/api/v2/documents/770e8400-e29b-41d4-a716-446655440002/presigned-url \
  -H "Authorization: Bearer <service-jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "expires_in_seconds": 300,
    "disposition": "inline"
  }'
```

**Требуемый scope:** `documents.read`

**Ответ (200):**

```json
{
  "url": "https://s3.example.com/documents-private/...?X-Amz-Signature=...",
  "expires_at": "2025-01-15T12:06:00Z",
  "file_id": "770e8400-e29b-41d4-a716-446655440002",
  "content_type": "application/pdf",
  "size_bytes": 1048576
}
```

> Presigned URL кэшируется в Redis. Параметр `expires_in_seconds` — от 1 до 600.

---

### Сценарий 5: Получение метаданных файла

#### По file_id

```bash
curl https://dss.example.com/api/v2/documents/770e8400-e29b-41d4-a716-446655440002 \
  -H "Authorization: Bearer <service-jwt>"
```

**Требуемый scope:** `documents.read`

**Ответ (200):** `FileResponse` (см. [Схемы ответов](#схемы-ответов)).

#### Список файлов по бизнес-сущности

```bash
curl "https://dss.example.com/api/v2/documents/by-owner/APPLICATION/550e8400-e29b-41d4-a716-446655440000?av_status=CLEAN&page=1&page_size=20" \
  -H "Authorization: Bearer <service-jwt>"
```

**Ответ (200):**

```json
{
  "items": [ ... ],
  "total": 5,
  "page": 1,
  "page_size": 20
}
```

Query-параметры:

| Параметр | По умолчанию | Описание |
|----------|-------------|---------|
| `av_status` | — | Фильтр по статусу AV: `PENDING`, `SCANNING`, `CLEAN`, `INFECTED`, `ERROR` |
| `page` | 1 | Номер страницы (>= 1) |
| `page_size` | 20 | Размер страницы (1–100) |

> Возвращаются только актуальные версии (`is_latest = true`), не удалённые файлы.

---

### Сценарий 6: Удаление файла

```bash
curl -X DELETE https://dss.example.com/api/v2/documents/770e8400-e29b-41d4-a716-446655440002 \
  -H "Authorization: Bearer <service-jwt>"
```

**Требуемый scope:** `documents.delete`

**Ответ:** `204 No Content`

> Удаление мягкое (soft delete) — запись получает `deleted_at`, файл перестаёт возвращаться в выборках.

---

### Сценарий 7: Скачивание публичного файла

Для публичных документов (извещения каталога) авторизация не требуется:

```bash
curl -L https://dss.example.com/api/v2/documents/public/770e8400-e29b-41d4-a716-446655440002/download
```

**Ответ:** `302 Redirect` на presigned S3 URL.

**Условия:** файл должен иметь `visibility = "PUBLIC"` и `av_status = "CLEAN"`.

Rate limit: 200 запросов/мин на IP.

---

### Схемы ответов

#### FileResponse

```json
{
  "file_id": "UUID",
  "original_name": "string",
  "content_type": "string (MIME)",
  "size_bytes": 0,
  "checksum_sha256": "string (64 hex)",
  "checksum_gost": "string (64 hex, GOST 34.11-2012-256)",
  "av_status": "PENDING | SCANNING | CLEAN | INFECTED | ERROR",
  "version": 1,
  "visibility": "PRIVATE | PUBLIC",
  "owner_type": "string",
  "owner_id": "UUID",
  "uploaded_by": "UUID",
  "uploaded_at": "datetime ISO8601"
}
```

#### FileListResponse

```json
{
  "items": [ FileResponse, ... ],
  "total": 0,
  "page": 1,
  "page_size": 20
}
```

---

### Коды ошибок

Все ошибки возвращаются в формате `{"detail": "описание ошибки"}`.

| HTTP код | Когда |
|----------|-------|
| **400** | Невалидные параметры, отсутствуют обязательные поля (напр. `owner_type` при M2M-загрузке) |
| **401** | Невалидный/просроченный JWT или upload/download-token |
| **403** | Отсутствует необходимый scope; файл не прошёл AV-проверку (`AVNotPassedError`) |
| **404** | Файл не найден |
| **413** | Размер файла превышает лимит (20 МБ по умолчанию или `max_size_bytes` из upload-token) |
| **415** | Недопустимый MIME-тип. Разрешены: PDF, DOCX, XLSX, JPEG, PNG |
| **503** | Readiness check — один из компонентов (PostgreSQL, Redis, S3) недоступен |

---

### Жизненный цикл файла и AV-статусы

```
   Upload
     │
     ▼
  PENDING ──> SCANNING ──┬──> CLEAN ──> Файл доступен для скачивания
                         │
                         ├──> INFECTED ──> Файл заблокирован
                         │
                         └──> ERROR ──> Требуется повторная проверка
```

1. При загрузке файл помещается в бакет `documents-quarantine` со статусом `SCANNING`
2. DSS публикует задачу в Kafka-топик `documents.av.scan.request`
3. AV Worker скачивает файл, сканирует через ClamAV, публикует результат в `documents.av.scan.result`
4. DSS обновляет статус и при `CLEAN` — перемещает файл в целевой бакет (`documents-private` или `documents-public`)

> **Файлы со статусом отличным от `CLEAN` недоступны для скачивания.** Запрос download-token или presigned URL вернёт 403.

---

### Kafka: события и топики

DSS публикует события в Kafka, которые могут быть полезны для интегрирующихся сервисов.

#### Топик `documents.events` — события жизненного цикла

Подпишитесь на этот топик, чтобы реагировать на изменения статуса файлов.

**Формат сообщения:**

```json
{
  "event_type": "FILE_AV_PASSED",
  "file_id": "770e8400-e29b-41d4-a716-446655440002",
  "owner_type": "APPLICATION",
  "owner_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-01-15T12:02:00Z",
  "correlation_id": "request-uuid",
  "actor_id": "user-uuid | null",
  "details": { ... }
}
```

**Ключ сообщения:** `file_id` (строка UUID).

**Типы событий:**

| event_type | Когда | Что в details |
|------------|-------|--------------|
| `FILE_AV_PASSED` | Файл прошёл AV-проверку | `{"av_engine": "ClamAV", "scan_duration_ms": 150}` |
| `FILE_AV_FAILED` | Обнаружена угроза | `{"threats": ["Win.Trojan.Agent"]}` |
| `FILE_MOVED_TO_STORAGE` | Файл перемещён из карантина в целевой бакет | `{"bucket": "documents-private", "storage_key": "..."}` |

**Рекомендация для интеграторов:**

Подпишитесь на `FILE_AV_PASSED` с фильтрацией по `owner_type` и `owner_id`, чтобы узнать, когда файл готов к использованию. Это позволит обновить статус бизнес-сущности (например, пометить заявку как «документы загружены»).

#### Внутренние топики (не для внешнего использования)

| Топик | Назначение |
|-------|-----------|
| `documents.av.scan.request` | Внутренний: задачи для AV Worker |
| `documents.av.scan.result` | Внутренний: результаты AV-сканирования |
| `audit.document_storage` | Аудит-лог (формат по согласованию) |

---

### Correlation ID

DSS поддерживает сквозную трассировку через заголовок `X-Correlation-Id`.

- Если вызывающий сервис передаёт `X-Correlation-Id`, DSS использует его во всех внутренних операциях и Kafka-событиях
- Если заголовок не передан, DSS генерирует UUID автоматически
- Тот же `correlation_id` присутствует в Kafka-событиях — используйте его для связи событий с вашими запросами

```bash
curl -X POST https://dss.example.com/api/v2/documents/upload-token \
  -H "Authorization: Bearer <service-jwt>" \
  -H "X-Correlation-Id: your-request-uuid" \
  -H "Content-Type: application/json" \
  -d '{ ... }'
```

---

### Health Checks

```bash
# Liveness probe (для K8s)
curl https://dss.example.com/health
# {"status": "ok"}

# Readiness probe (проверяет PostgreSQL, Redis, S3)
curl https://dss.example.com/health/ready
# {"postgres": "ok", "redis": "ok", "s3": "ok"}
# HTTP 503 если хотя бы один компонент недоступен
```

### Метрики

Prometheus метрики доступны по адресу `/metrics`.

---

### S3 Bucket Layout

| Bucket | Назначение | Шифрование | Lifecycle |
|--------|-----------|-----------|-----------|
| `documents-private` | Приватные файлы (заявки, протоколы) | SSE-S3 | Версионирование |
| `documents-public` | Публичные документы извещений | SSE-S3 | — |
| `documents-quarantine` | Временное хранение до AV scan | SSE-S3 | Auto-delete 24h |

Ключи объектов:
```
documents-private/{owner_type}/{owner_id}/{file_id}/{version}
documents-public/{notice_id}/{lot_id}/{file_id}
documents-quarantine/{upload_id}
```

---

### Ограничения

| Параметр | Значение по умолчанию |
|----------|----------------------|
| Максимальный размер файла | 20 МБ |
| Допустимые MIME-типы | `application/pdf`, `application/vnd.openxmlformats-officedocument.wordprocessingml.document` (DOCX), `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` (XLSX), `image/jpeg`, `image/png` |
| TTL upload-token | 10 минут |
| TTL download-token | до 10 минут (задаётся при запросе) |
| TTL presigned URL | 5 минут (приватные), 1 час (публичные) |
| Rate limit upload | 30 запросов / 5 минут (на пользователя) |
| Rate limit download | 100 запросов / минуту (на пользователя) |
| Rate limit публичные | 200 запросов / минуту (на IP) |
| Rate limit M2M API | 1000 запросов / минуту (на сервис) |

---

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
