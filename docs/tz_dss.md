# Document Storage Service — Архитектурный проект

**Версия:** 2.3  
**Дата:** 2025-10-20  
**Статус:** Проект  
**Владелец:** Команда документооборота / Команда инфраструктуры

---

## 1. Назначение и контекст

Document Storage Service (далее — DSS) — микросервис хранения, управления и выдачи файлов электронной торговой площадки. Сервис является критической зависимостью для большинства бизнес-процессов ЭТП: подача заявок, публикация извещений, формирование протоколов, возврат средств, приостановка торгов.

DSS выступает единой точкой входа для работы с файлами, инкапсулируя взаимодействие с S3-совместимым хранилищем, антивирусной проверкой, шифрованием и раздачей через CDN. В качестве хранилища используется любой S3-совместимый провайдер (Timeweb S3, AWS S3, и т.д.), для локальной разработки и тестов — MinIO.

### 1.1. Ключевые потребители

| Сервис-потребитель | Сценарии использования | Критичность |
|---|---|---|
| **Application Service** | Загрузка вложений заявки (устав, доверенности), привязка к черновику | Высокая |
| **Notice Service** | Документы извещений и лотов, версионность при обновлениях | Высокая |
| **DocGen Service** | Сохранение сгенерированных протоколов (допуска, итоговых) | Высокая |
| **Crypto EDS** | Получение файла по URL для вычисления хеша и проверки подписи | Высокая |
| **Refund Service** | Загрузка подтверждающих документов при возврате средств | Средняя |
| **Public Catalog / CDN** | Раздача публичных документов извещений гостям и участникам | Высокая |

### 1.2. Требования из use case'ов

| Требование | Источник |
|---|---|
| Таймаут обращения к DSS ≤ 2 секунды | UC-018 (Participant Admission), UC-020 (Submit Application) |
| Fallback на резервное хранилище при сбое | UC-018, UC-004 (View Public Catalog) |
| Антивирусная проверка всех загружаемых файлов | UC-020 (Шаг 3), UC-003 (Update Notice) |
| Шифрование файлов at-rest (AES-256) | UC-008 (Admission Protocol), UC-020 |
| Хранение файлов ≥ 5 лет | UC-020, UC-003, UC-008, 44-ФЗ/223-ФЗ |
| Допустимые форматы: PDF, DOCX, XLSX, JPG, PNG | UC-020 (БП-123) |
| Общий объём вложений ≤ 200 МБ (на заявку), ≤ 300 МБ (на извещение) | UC-020, UC-003 |
| Раздача публичных документов через CDN с мониторингом SLA | UC-004 (БП-416) |
| RBAC — доступ к файлам по ролям на уровне лота/заявки | UC-018 |
| Максимальный размер файла — строго ≤ 20 МБ; chunked upload не поддерживается | UC-020 (Шаг 3), архитектурное решение |
| Контроль целостности (SHA-256 checksum) | UC-022 (Suspend Auction), UC-020 |
| Хранение результатов антивирусной проверки вместе с файлом | UC-020 (БП-137) |
| Привязка файлов к бизнес-сущностям (заявка, лот, извещение, решение) | Все UC |
| Хранение ЭЦП и результатов проверки подписей вместе с документами | UC-020 (Шаг 5), UC-022 |

---

## 2. Технологический стек

| Компонент | Технология | Обоснование |
|---|---|---|
| Язык | Python 3.12 | Единый стек с Crypto EDS и другими сервисами |
| Web-фреймворк | FastAPI | Async, автодокументация OpenAPI, Pydantic v2 |
| S3-клиент | aiobotocore / boto3 | Стандарт для S3-совместимых хранилищ, async |
| Object Storage | S3-совместимое хранилище | Прод: Timeweb S3 / иной провайдер. Dev/test: MinIO |
| ГОСТ-хеширование | pygost | Чистый Python, ГОСТ 34.11-2012 (Стрибог), без зависимости на КриптоПро |
| Метаданные (БД) | PostgreSQL 16 | Хранение метаданных файлов, привязок, подписей |
| ORM / Query | SQLAlchemy 2.0 (async) + asyncpg | Async ORM, миграции через Alembic |
| Кеш | Redis 7 | Кеш presigned URL, rate limiting, дедупликация |
| Очередь | Apache Kafka | Асинхронные задачи: AV-scan, события аудита |
| Антивирус | ClamAV (clamd) | Open-source, интеграция через unix socket / TCP |
| HTTP-клиент | aiohttp | Async HTTP для интеграций |
| Валидация | Pydantic v2 | Модели запросов/ответов |
| Логирование | structlog (JSON) | Единый формат с Crypto EDS |
| Метрики | prometheus-client | Единый мониторинг |
| Миграции | Alembic | Версионирование схемы БД |
| Контейнеризация | Docker (Ubuntu 24.04) | Единый базовый образ |

---

## 3. Архитектура

### 3.1. Слои приложения

```
API Layer           routes_upload, routes_upload_token, routes_download, routes_metadata, routes_public
  ↓
Domain Layer        file_service, signature_service, models
  ↓
Storage Layer       s3_client, metadata_repository, cache_client
  ↓
Infrastructure      Kafka (audit, AV tasks), ClamAV, Redis, PostgreSQL, Prometheus
```

### 3.2. Компонентная диаграмма

```
                         ┌─────────────────────────────────────────────┐
                         │           Document Storage Service          │
                         │                                             │
  ┌──────────┐           │  ┌────────────┐    ┌──────────────────┐     │
  │ Web-клиент│──upload──►│  │ API Layer  │───►│  Domain Layer    │     │
  │          │◄─download─│  │ (FastAPI)  │◄───│  (file_service)  │     │
  └──────────┘           │  └─────┬──────┘    └────────┬─────────┘     │
                         │        │                    │               │
                         │        │ presigned URL      │               │
                         │        ▼                    ▼               │
                         │  ┌────────────┐    ┌──────────────────┐     │
                         │  │  Redis     │    │  Storage Layer   │     │
                         │  │  (cache)   │    │                  │     │
                         │  └────────────┘    │  ┌────────────┐  │     │
                         │                    │  │ S3 Client  │  │     │
                         │                    │  │            │  │     │
                         │                    │  └────────────┘  │     │
                         │                    │  ┌────────────┐  │     │
  ┌──────────┐           │                    │  │ PostgreSQL │  │     │
  │ Notice   │──prepare──│                    │  │ (metadata) │  │     │
  │ Service  │◄─token────│                    │  └────────────┘  │     │
  │          │──confirm──►│                    └──────────────────┘     │
  └──────────┘           │                           │                 │
                         │                    ┌──────┴───────┐         │
                         │                    │    Kafka      │         │
                         │                    │ (AV, audit)   │         │
                         │                    └──────┬───────┘         │
                         └───────────────────────────┼─────────────────┘
                                                     │
                                              ┌──────┴───────┐
                                              │   ClamAV     │
                                              │ (AV worker)  │
                                              └──────────────┘
```

### 3.3. Паттерн интеграции: двухфазная загрузка (claim check)

DSS работает по паттерну «claim check» — сервисы-потребители (Notice Service, Application Service и т.д.) **не проксируют файловый поток** через себя. Вместо этого используется двухфазный протокол:

```
                          ┌──── (1) бизнес-проверка ────► Notice Service
                          │                                    │
Web-клиент ───────────────┤                              (2) upload_token
                          │                                    │
                          │                                    ▼
                          └──── (3) file + token ──────► DSS API
                                                           │
                                                     (4) file_id
                          ┌────────────────────────────────┘
                          │
                          └──── (5) confirm file_id ───► Notice Service
```

**Фаза 1 — получение разрешения.** Web-клиент запрашивает у бизнес-сервиса (Notice Service, Application Service) разрешение на загрузку. Бизнес-сервис проверяет свои правила (статус сущности, права пользователя, не заблокирована ли сущность) и обращается к DSS за upload-токеном:

```
POST /api/v2/documents/upload-token
{
  "owner_type": "LOT",
  "owner_id": "lot-uuid-123",
  "visibility": "PUBLIC",
  "file_name": "charter.pdf",
  "content_type": "application/pdf",
  "max_size_bytes": 20971520
}
```

DSS генерирует JWT upload-token (TTL 10 минут), содержащий `owner_type`, `owner_id`, `visibility`, `max_size_bytes`, `exp`. Бизнес-сервис возвращает токен клиенту.

**Фаза 2 — прямая загрузка.** Web-клиент отправляет файл напрямую в DSS, прикладывая upload-token. DSS валидирует подпись токена, проверяет срок действия, извлекает параметры привязки. Файл не проходит через бизнес-сервис — ни одного лишнего сетевого хопа.

**Фаза 3 — подтверждение.** Клиент сообщает бизнес-сервису `file_id`, полученный от DSS. Бизнес-сервис верифицирует привязку (опционально запрашивая метаданные у DSS) и сохраняет `file_id` в своей модели данных.

**Преимущества:**
- Бизнес-сервисы не держат файловые байты в памяти и не становятся бутылочным горлышком.
- DSS остаётся единственным сервисом, работающим с S3 — упрощается аудит и мониторинг.
- Upload-token обеспечивает авторизацию без дополнительных вызовов IAM на стороне DSS.

**Для M2M-вызовов** (например, DocGen Service сохраняет сгенерированный протокол) бизнес-сервис может использовать сервисный JWT со scope `documents.write` и загружать файл в DSS напрямую без upload-token.

### 3.3. Стратегия хранения в S3 (bucket layout)

DSS использует несколько бакетов с разным уровнем доступа и retention-политиками:

```
documents-private/          ← Приватные файлы (заявки, протоколы, решения)
  ├── {owner_type}/{owner_id}/{file_id}/{version}
  │     например: application/app-uuid/file-uuid/1
  └── ...

documents-public/           ← Публичные документы извещений (каталог)
  ├── {notice_id}/{lot_id}/{file_id}
  └── ...

documents-quarantine/       ← Карантин до завершения антивирусной проверки
  ├── {upload_id}
  └── ...
```

**Политики бакетов:**

| Бакет | Шифрование | Versioning | Lifecycle | Доступ |
|---|---|---|---|---|
| `documents-private` | SSE-S3 (AES-256) | Включено | Хранение ≥ 5 лет, переход в cold-tier через 1 год | Только через DSS API |
| `documents-public` | SSE-S3 | Включено | Хранение ≥ 3 года | Presigned URL / CDN |
| `documents-quarantine` | SSE-S3 | Выключено | Автоудаление через 24 часа | Только DSS внутренний |

Конфигурация бакетов не зависит от провайдера — SSE-S3, versioning и lifecycle поддерживаются всеми S3-совместимыми хранилищами (Timeweb S3, AWS S3, MinIO). Конкретные параметры (endpoint, credentials, region) задаются через переменные окружения.

Электронные подписи (CAdES/PKCS#7) хранятся в PostgreSQL в виде base64-строки (поле `signature_b64` в таблице `file_signatures`), а не как файлы в S3. Типичный размер подписи — 2–10 КБ в base64, что укладывается в лимиты PostgreSQL и не оправдывает отдельный бакет с lifecycle-политиками.

---

## 4. Модель данных (PostgreSQL)

### 4.1. Таблица `files` — метаданные файлов

```sql
CREATE TABLE files (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Идентификация файла
    original_name   VARCHAR(500)    NOT NULL,
    storage_key     VARCHAR(1000)   NOT NULL,       -- S3 key
    bucket          VARCHAR(100)    NOT NULL,
    content_type    VARCHAR(255)    NOT NULL,        -- MIME type
    size_bytes      BIGINT          NOT NULL,
    
    -- Контроль целостности
    checksum_sha256 VARCHAR(64)     NOT NULL,        -- SHA-256 (контроль целостности S3)
    checksum_gost   VARCHAR(64)     NOT NULL,        -- ГОСТ 34.11-2012-256 (Стрибог, для проверки ЭЦП)
    
    -- Привязка к бизнес-сущности (один файл = одна сущность)
    owner_type      VARCHAR(50)     NOT NULL,        -- APPLICATION, LOT, NOTICE, DECISION, REFUND, PROTOCOL
    owner_id        UUID            NOT NULL,
    
    -- Версионирование
    version         INT             NOT NULL DEFAULT 1,
    s3_version_id   VARCHAR(255),                    -- S3 version ID
    is_latest       BOOLEAN         NOT NULL DEFAULT TRUE,
    previous_version_id UUID        REFERENCES files(id),
    
    -- Классификация
    visibility      VARCHAR(20)     NOT NULL DEFAULT 'PRIVATE',  -- PRIVATE | PUBLIC
    
    -- Антивирус
    av_status       VARCHAR(20)     NOT NULL DEFAULT 'PENDING',  -- PENDING | SCANNING | CLEAN | INFECTED | ERROR
    av_scanned_at   TIMESTAMPTZ,
    av_engine       VARCHAR(100),
    av_report       JSONB,                           -- Подробный отчёт ClamAV
    
    -- Аудит
    uploaded_by     UUID            NOT NULL,         -- user_id
    uploaded_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    deleted_at      TIMESTAMPTZ,                     -- soft delete
    
    -- Служебные
    correlation_id  UUID,
    metadata        JSONB           DEFAULT '{}'::JSONB,
    
    CONSTRAINT chk_visibility CHECK (visibility IN ('PRIVATE', 'PUBLIC')),
    CONSTRAINT chk_av_status CHECK (av_status IN ('PENDING', 'SCANNING', 'CLEAN', 'INFECTED', 'ERROR'))
);

CREATE INDEX idx_files_storage_key ON files(storage_key);
CREATE INDEX idx_files_owner ON files(owner_type, owner_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_files_uploaded_by ON files(uploaded_by);
CREATE INDEX idx_files_av_status ON files(av_status) WHERE av_status != 'CLEAN';
CREATE INDEX idx_files_visibility ON files(visibility) WHERE visibility = 'PUBLIC';
```

### 4.2. Таблица `file_signatures` — хранение ЭЦП и результатов проверки

```sql
CREATE TABLE file_signatures (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id             UUID            NOT NULL REFERENCES files(id),
    
    -- Данные подписи (хранится в БД, не в S3)
    signature_b64       TEXT            NOT NULL,  -- Подпись в base64 (CAdES/PKCS#7, типичный размер 2–10 КБ)
    signature_format    VARCHAR(30)     NOT NULL,  -- CADES_BES, CADES_T, CADES_XLT1, PKCS7
    hash_algorithm      VARCHAR(50)     NOT NULL,  -- gostr3411_2012_256, sha256, etc.
    document_hash       VARCHAR(128)    NOT NULL,  -- Хеш подписанного документа
    
    -- Подписант
    signer_id           UUID            NOT NULL,
    signer_name         VARCHAR(500),
    certificate_serial  VARCHAR(100)    NOT NULL,
    certificate_issuer  VARCHAR(500),
    certificate_valid_from TIMESTAMPTZ,
    certificate_valid_to   TIMESTAMPTZ,
    is_qualified        BOOLEAN,                   -- Квалифицированная ЭЦП
    
    -- Результат проверки
    verification_status VARCHAR(20)     NOT NULL,  -- VALID, INVALID, CERT_INVALID, ERROR, PENDING
    verified_at         TIMESTAMPTZ,
    verification_report JSONB,                     -- Полный отчёт от Crypto EDS
    
    -- Временная метка (TSA)
    signing_time        TIMESTAMPTZ,
    tsa_token           TEXT,
    timestamp_time      TIMESTAMPTZ,
    
    -- Аудит
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    correlation_id      UUID,
    
    CONSTRAINT chk_sig_format CHECK (signature_format IN ('CADES_BES', 'CADES_T', 'CADES_XLT1', 'PKCS7')),
    CONSTRAINT chk_verification CHECK (verification_status IN ('VALID', 'INVALID', 'CERT_INVALID', 'ERROR', 'PENDING'))
);

CREATE INDEX idx_signatures_file ON file_signatures(file_id);
CREATE INDEX idx_signatures_signer ON file_signatures(signer_id);
CREATE INDEX idx_signatures_cert ON file_signatures(certificate_serial);
```

### 4.3. Диаграмма связей (ER)

```
┌──────────────────┐
│      files       │
│                  │
│ id               │
│ original_name    │
│ storage_key      │
│ owner_type       │
│ owner_id         │
│ checksum_sha256  │
│ checksum_gost    │
│ av_status        │
│ visibility       │
│ version          │
│ previous_version_id ──► files.id
└────────┬─────────┘
         │
         │ 1:N
         ▼
┌──────────────────┐
│ file_signatures  │
│                  │
│ file_id          │
│ signature_b64    │
│ signer_id        │
│ certificate_serial│
│ verification_status│
└──────────────────┘
```

---

## 5. API Specification

Базовый путь: `/api/v2/documents`

Аутентификация — три режима, чётко разделённые по типу вызывающей стороны:

- **Upload-token** (`X-Upload-Token: <JWT>`) — Web-клиент загружает файл. Только `POST /upload`.
- **Download-token** (`GET /download?token=<JWT>`) — Web-клиент скачивает приватный файл. Только `GET /download`.
- **Без аутентификации** — скачивание публичных файлов. Только `GET /public/{fileId}/download`.
- **Сервисный JWT** (`Authorization: Bearer <service-JWT>`) — все остальные эндпоинты. Только M2M-вызовы от бизнес-сервисов.

Web-клиент **не имеет прямого доступа** к эндпоинтам метаданных, привязок, подписей, версий и токенов. Эти эндпоинты закрыты за сервисным JWT и доступны только бизнес-сервисам.

Все эндпоинты поддерживают `X-Correlation-Id` для сквозной трассировки.

### 5.1. Загрузка файлов

#### 5.1.0. Получение upload-token (вызывается бизнес-сервисом)

```
POST /api/v2/documents/upload-token
Content-Type: application/json
Authorization: Bearer <service-JWT>
```

**Запрос:**

```json
{
  "owner_type": "LOT",
  "owner_id": "lot-uuid-123",
  "visibility": "PUBLIC",
  "file_name": "charter.pdf",
  "content_type": "application/pdf",
  "max_size_bytes": 20971520,
  "uploaded_by": "user-uuid"
}
```

**Ответ (201):**

```json
{
  "upload_token": "eyJhbGciOiJIUzI1NiIs...",
  "expires_at": "2025-10-20T12:10:00Z",
  "owner_type": "LOT",
  "owner_id": "lot-uuid-123"
}
```

**Логика:**
- DSS проверяет сервисный JWT вызывающего сервиса (scope `documents.issue_token`).
- Генерирует JWT upload-token (подписанный секретом DSS, TTL 10 минут).
- Payload токена: `owner_type`, `owner_id`, `visibility`, `max_size_bytes`, `uploaded_by`, `exp`.
- Токен одноразовый — после использования его `jti` помещается в Redis blacklist на оставшееся время TTL.

#### 5.1.1. Простая загрузка (≤ 20 МБ)

```
POST /api/v2/documents/upload
Content-Type: multipart/form-data
X-Upload-Token: eyJhbGciOiJIUzI1NiIs...
```

**Параметры формы:**

| Поле | Тип | Обязательно | Описание |
|---|---|---|---|
| `file` | binary | да | Файл |

Параметры привязки (`owner_type`, `owner_id`, `visibility`) **извлекаются из upload-token**. Клиент передаёт только файл.

При M2M-вызовах (DocGen Service, etc.) вместо `X-Upload-Token` используется `Authorization: Bearer <service-JWT>` со scope `documents.write`, и параметры привязки передаются в форме:

| Поле | Тип | Обязательно | Описание |
|---|---|---|---|
| `file` | binary | да | Файл |
| `owner_type` | string | да (M2M) | Тип сущности: `APPLICATION`, `LOT`, `NOTICE`, `DECISION`, `REFUND`, `PROTOCOL` |
| `owner_id` | string (UUID) | да (M2M) | ID сущности-владельца |
| `visibility` | string | нет | `PRIVATE` (по умолчанию) или `PUBLIC` |

**Ответ (201):**

```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "original_name": "charter.pdf",
  "content_type": "application/pdf",
  "size_bytes": 1048576,
  "checksum_sha256": "a1b2c3d4e5f6...",
  "checksum_gost": "7d9e8f0a1b2c...",
  "av_status": "PENDING",
  "version": 1,
  "visibility": "PUBLIC",
  "owner_type": "LOT",
  "owner_id": "lot-uuid-123",
  "uploaded_at": "2025-10-20T12:00:00Z"
}
```

**Бизнес-правила:**
- Валидация upload-token: подпись, срок, одноразовость (jti в Redis blacklist).
- Проверка формата файла (white-list: PDF, DOCX, XLSX, JPG, PNG).
- Проверка размера ≤ `max_size_bytes` из токена.
- Вычисление двух хешей в одном streaming-проходе (без загрузки файла в память целиком):
  - **SHA-256** — для контроля целостности при копировании между бакетами S3.
  - **ГОСТ 34.11-2012-256** (Стрибог, через pygost) — для последующей проверки электронных подписей. Этот хеш передаётся в Crypto EDS в режиме `sync_hash`, избегая повторного скачивания файла.
- Загрузка в карантинный бакет `documents-quarantine`.
- Публикация задачи антивирусной проверки в Kafka.
- После прохождения AV — перемещение в целевой бакет.

### 5.2. Скачивание файлов

#### 5.2.0. Получение download-token (вызывается бизнес-сервисом)

```
POST /api/v2/documents/download-token
Content-Type: application/json
Authorization: Bearer <service-JWT>
```

**Запрос:**

```json
{
  "file_id": "file-uuid-789",
  "user_id": "user-uuid",
  "version": null,
  "disposition": "inline",
  "expires_in_seconds": 300
}
```

**Ответ (201):**

```json
{
  "download_token": "eyJhbGciOiJIUzI1NiIs...",
  "expires_at": "2025-10-20T12:05:00Z",
  "file_id": "file-uuid-789",
  "original_name": "charter.pdf",
  "content_type": "application/pdf",
  "size_bytes": 1048576
}
```

**Логика:**
- DSS проверяет сервисный JWT вызывающего бизнес-сервиса (scope `documents.issue_token`).
- Проверяет, что файл существует и `av_status = CLEAN` (для приватных файлов).
- Генерирует JWT download-token (TTL из `expires_in_seconds`, максимум 10 минут).
- Payload: `jti`, `file_id`, `version`, `disposition`, `user_id`, `exp`.
- Токен одноразовый — после использования `jti` помещается в Redis blacklist.

**Ключевой принцип:** DSS **не проверяет**, имеет ли `user_id` право скачивать файл. Эту проверку выполнил бизнес-сервис до запроса токена. Например, Notice Service знает, что лот ещё не опубликован, и выдаст download-token только организатору этого лота. Application Service знает, что заявка принадлежит конкретному участнику, и выдаст токен только ему.

#### 5.2.1. Скачивание приватного файла

```
GET /api/v2/documents/download?token={download_token}
```

**Ответ:** Redirect 302 на presigned S3 URL (TTL из токена).

DSS валидирует download-token (подпись, срок, одноразовость через Redis blacklist), генерирует presigned URL к S3 и выполняет redirect. Никакой дополнительной авторизации не требуется — наличие валидного токена является достаточным подтверждением права доступа.

#### 5.2.2. Получение presigned URL (M2M)

```
POST /api/v2/documents/{fileId}/presigned-url
Authorization: Bearer <service-JWT>
```

**Запрос:**

```json
{
  "expires_in_seconds": 300,
  "disposition": "inline"
}
```

**Ответ (200):**

```json
{
  "url": "https://<s3-endpoint>/documents-private/...?X-Amz-...",
  "expires_at": "2025-10-20T12:05:00Z",
  "file_id": "file-uuid",
  "content_type": "application/pdf",
  "size_bytes": 1048576
}
```

Используется бизнес-сервисами напрямую (например, Crypto EDS получает URL файла для проверки подписи). Требует scope `documents.read`.

#### 5.2.3. Публичный доступ к документам (для каталога)

```
GET /api/v2/documents/public/{fileId}/download
```

Не требует аутентификации и не требует download-token. Работает только для файлов с `visibility = PUBLIC` и `av_status = CLEAN`. Поддерживает кеширование через `Cache-Control`, `ETag`, `If-None-Match`.

### 5.3. Метаданные и управление

#### 5.3.1. Получение метаданных файла

```
GET /api/v2/documents/{fileId}
```

**Ответ (200):**

```json
{
  "file_id": "file-uuid",
  "original_name": "charter.pdf",
  "content_type": "application/pdf",
  "size_bytes": 1048576,
  "checksum_sha256": "a1b2c3...",
  "checksum_gost": "7d9e8f...",
  "version": 1,
  "visibility": "PRIVATE",
  "av_status": "CLEAN",
  "av_scanned_at": "2025-10-20T12:00:30Z",
  "uploaded_by": "user-uuid",
  "uploaded_at": "2025-10-20T12:00:00Z",
  "owner_type": "APPLICATION",
  "owner_id": "app-uuid",
  "signatures": [
    {
      "signature_id": "sig-uuid",
      "signer_name": "Иванов Иван Иванович",
      "certificate_serial": "01AB23CD",
      "is_qualified": true,
      "verification_status": "VALID",
      "signing_time": "2025-10-20T12:01:00Z"
    }
  ]
}
```

#### 5.3.2. Список файлов по сущности

```
GET /api/v2/documents/by-owner/{ownerType}/{ownerId}
```

**Query-параметры:**

| Параметр | Тип | Описание |
|---|---|---|
| `av_status` | string | Фильтр по AV-статусу |
| `include_signatures` | bool | Включить данные подписей |
| `page` | int | Страница (по умолчанию 1) |
| `page_size` | int | Размер страницы (по умолчанию 20, макс 100) |

**Ответ (200):**

```json
{
  "items": [ /* массив файлов с метаданными */ ],
  "total": 5,
  "page": 1,
  "page_size": 20
}
```

#### 5.3.3. Удаление файла (soft delete)

```
DELETE /api/v2/documents/{fileId}
```

Устанавливает `deleted_at`. Файл остаётся в S3 согласно retention-политике (≥ 5 лет). Доступен администраторам для аудита.

### 5.4. Электронные подписи

#### 5.4.1. Привязка подписи к файлу (синхронная проверка)

```
POST /api/v2/documents/{fileId}/signatures
Authorization: Bearer <service-JWT>
```

**Запрос:**

```json
{
  "signature_b64": "MIIBxwYJKo...",
  "signature_format": "CADES_BES",
  "hash_algorithm": "gostr3411_2012_256",
  "signer_id": "user-uuid",
  "certificate_serial": "01AB23CD"
}
```

Бизнес-сервис выбирает этот путь, когда ему нужен результат верификации немедленно (участник подписывает заявку и ждёт ответа, организатор подписывает извещение перед публикацией).

**Логика DSS:**
1. Читает `checksum_gost` из таблицы `files` для данного `fileId`.
2. Вызывает Crypto EDS синхронно по HTTP:
   ```json
   POST /api/v1/verify
   {
     "signature_b64": "MIIBxwYJKo...",
     "hash_b64": "<checksum_gost в base64>",
     "hash_algorithm": "gostr3411_2012_256",
     "check_cert_status": true
   }
   ```
3. Crypto EDS проверяет подпись по ГОСТ-хешу (без скачивания файла из S3) и возвращает результат.
4. DSS сохраняет `signature_b64` + результат верификации в таблицу `file_signatures`.
5. Публикует событие `SIGNATURE_ADDED` в Kafka.

**Ответ (201):**

```json
{
  "signature_id": "sig-uuid",
  "file_id": "file-uuid",
  "verification_status": "VALID",
  "signer_name": "Иванов Иван Иванович",
  "is_qualified": true,
  "signing_time": "2025-10-20T12:01:00Z",
  "certificate_valid_to": "2025-12-31T23:59:59Z"
}
```

Если подпись невалидна — DSS всё равно сохраняет запись (с `verification_status = INVALID` или `CERT_INVALID`) для аудита. Бизнес-сервис получает результат и решает, как реагировать.

#### 5.4.2. Привязка подписи к файлу (асинхронная проверка через Kafka)

Бизнес-сервис выбирает этот путь, когда не нужен мгновенный результат: массовое подписание протоколов, фоновые операции, ситуации когда допустима задержка.

Бизнес-сервис публикует сообщение в Kafka-топик `documents.signatures.request`:

```json
{
  "task_id": "uuid",
  "file_id": "file-uuid-789",
  "signature_b64": "MIIBxwYJKo...",
  "signature_format": "CADES_BES",
  "hash_algorithm": "gostr3411_2012_256",
  "signer_id": "user-uuid",
  "certificate_serial": "01AB23CD",
  "correlation_id": "corr-uuid"
}
```

**Логика DSS (consumer):**
1. Потребляет сообщение из `documents.signatures.request`.
2. Сохраняет `signature_b64` в `file_signatures` с `verification_status = PENDING`.
3. Читает `checksum_gost` из `files` и отправляет задачу проверки в `tasks.crypto_eds`.
4. При получении результата из `results.crypto_eds` — обновляет `file_signatures`.
5. Публикует событие `SIGNATURE_VERIFIED` в `documents.events` (бизнес-сервис может подписаться и отреагировать).

#### 5.4.3. Получение подписей файла

```
GET /api/v2/documents/{fileId}/signatures
```

Возвращает все подписи файла (append-only, старые не удаляются). Бизнес-сервис определяет «актуальную» подпись на своей стороне.

#### 5.4.4. Повторная проверка подписи

```
POST /api/v2/documents/{fileId}/signatures/{signatureId}/reverify
Authorization: Bearer <service-JWT>
```

Всегда выполняется асинхронно. DSS перечитывает `signature_b64` из `file_signatures`, отправляет задачу проверки в `tasks.crypto_eds`. Возвращает `202 Accepted`. При изменении `verification_status` публикуется событие `SIGNATURE_REVERIFIED` в `documents.events`.

Полезно при обновлении CRL или при оспаривании.

### 5.4.5. Выбор канала: когда HTTP, когда Kafka

DSS не определяет способ проверки — это ответственность бизнес-сервиса. Выбор канала определяется бизнес-требованиями:

| Сценарий | Канал | Почему |
|---|---|---|
| Участник подписывает заявку (UC-020) | HTTP sync | Участник ждёт ответа, статус заявки меняется сразу |
| Организатор подписывает извещение (UC-016) | HTTP sync | Публикация блокируется до подтверждения подписи |
| Подписание ценового предложения (UC-021) | HTTP sync | Критичная латency (≤ 2 сек) |
| Генерация протокола с подписью (UC-008, UC-015) | Kafka async | Массовая операция, результат не нужен мгновенно |
| Повторная проверка после обновления CRL | Kafka async (reverify) | Фоновая операция по расписанию |
| Победитель подписывает итоговый протокол (UC-015) | HTTP sync | Победитель ждёт подтверждения |

### 5.5. Версионирование

#### 5.5.1. Загрузка новой версии

```
POST /api/v2/documents/{fileId}/versions
Content-Type: multipart/form-data
Authorization: Bearer <service-JWT>
```

Создаёт новую версию файла. В таблице `files` появляется **новая запись** со своим `id`, новыми хешами и `previous_version_id` → старая запись. Старая версия получает `is_latest = false`.

**Что происходит с подписями при замене версии:**

Подпись — это криптографическая операция над конкретными байтами. Если байты изменились, старая подпись математически невалидна для нового содержимого: ГОСТ-хеш нового файла не совпадёт с хешем, который был подписан.

Поэтому:

1. **Старые подписи остаются привязанными к старой версии** (`file_signatures.file_id` → старый `files.id`). Они не удаляются и не переносятся — это аудитный след, доказывающий, что старая версия была подписана конкретным лицом в конкретное время.

2. **Новая версия создаётся без подписей.** Бизнес-сервис должен инициировать подписание заново — точно так же, как при первичной загрузке.

3. **Привязка к сущности наследуется.** Новая версия создаётся с теми же `owner_type` / `owner_id`, что у предыдущей. Бизнес-сервису не нужно перепривязывать файл — он запрашивает файлы по `owner_type/owner_id` и автоматически получает последнюю версию (`is_latest = true`).

**Логика DSS:**
1. Проверяет, что `fileId` существует и `is_latest = true`.
2. Принимает новый файл, вычисляет SHA-256 + ГОСТ-хеш, загружает в карантин.
3. Создаёт новую запись в `files`: `version = old.version + 1`, `previous_version_id = old.id`, `is_latest = true`, `owner_type = old.owner_type`, `owner_id = old.owner_id`.
4. Обновляет старую запись: `is_latest = false`.
5. Публикует AV-задачу в Kafka.
6. Публикует AV-задачу в Kafka.
7. Публикует событие `FILE_VERSION_CREATED` в `documents.events`.

**Ответ (201):**

```json
{
  "file_id": "new-file-uuid",
  "version": 2,
  "previous_version_id": "old-file-uuid",
  "original_name": "charter_v2.pdf",
  "checksum_sha256": "xyz...",
  "checksum_gost": "abc...",
  "av_status": "PENDING",
  "owner_type": "LOT",
  "owner_id": "lot-uuid",
  "signatures": []
}
```

**Пример: организатор обновляет документацию лота.**

```
Notice Service                   DSS                           PostgreSQL
  │                                │                               │
  │── POST /documents/{fileId}/    │                               │
  │   versions (новый PDF) ───────►│                               │
  │                                │── SELECT old file ───────────►│
  │                                │   (version=1, is_latest=true) │
  │                                │                               │
  │                                │── compute hashes ─────────────│
  │                                │── PUT S3 (quarantine) ────────│
  │                                │                               │
  │                                │── INSERT files ──────────────►│
  │                                │   (version=2, is_latest=true, │
  │                                │    previous_version_id=old,   │
  │                                │    owner_type/id = old.owner) │
  │                                │                               │
  │                                │── UPDATE files ──────────────►│
  │                                │   SET is_latest=false          │
  │                                │   WHERE id=old                │
  │                                │                               │
  │                                │   (file_signatures для old    │
  │                                │    НЕ трогаются — остаются    │
  │                                │    привязанными к old.id)     │
  │                                │                               │
  │◄── 201 {file_id: new, v: 2,  │                               │
  │    signatures: []} ────────────│                               │
  │                                │                               │
  │   Notice Service видит пустой  │                               │
  │   signatures[] → понимает, что │                               │
  │   нужно переподписать          │                               │
```

#### 5.5.2. Список версий

```
GET /api/v2/documents/{fileId}/versions
Authorization: Bearer <service-JWT>
```

Принимает `fileId` любой версии (текущей или старой) — DSS проходит по цепочке `previous_version_id` и собирает все версии.

**Ответ (200):**

```json
{
  "file_id": "new-file-uuid",
  "versions": [
    {
      "file_id": "new-file-uuid",
      "version": 2,
      "size_bytes": 1100000,
      "checksum_sha256": "xyz...",
      "checksum_gost": "abc...",
      "uploaded_at": "2025-10-20T14:00:00Z",
      "uploaded_by": "user-uuid",
      "is_latest": true,
      "signatures_count": 0
    },
    {
      "file_id": "old-file-uuid",
      "version": 1,
      "size_bytes": 1048576,
      "checksum_sha256": "a1b2c3...",
      "checksum_gost": "d4e5f6...",
      "uploaded_at": "2025-10-20T12:00:00Z",
      "uploaded_by": "user-uuid",
      "is_latest": false,
      "signatures_count": 1
    }
  ]
}
```

Бизнес-сервис может запросить подписи старой версии через `GET /documents/{old-file-uuid}/signatures` — они по-прежнему доступны для аудита.

### 5.6. Health и метрики

| Метод | Путь | Описание |
|---|---|---|
| GET | `/health` | Liveness — сервис отвечает |
| GET | `/health/ready` | Readiness — S3, PostgreSQL, Redis доступны |
| GET | `/metrics` | Prometheus-метрики |

---

## 6. Потоки данных

### 6.1. Загрузка файла (двухфазный upload через upload-token)

```
Web-клиент          Notice Service         DSS API              S3         Kafka           ClamAV Worker
  │                      │                    │                     │                 │                  │
  │── (1) POST /lots/    │                    │                     │                 │                  │
  │   {lotId}/documents/ │                    │                     │                 │                  │
  │   prepare ──────────►│                    │                     │                 │                  │
  │                      │── check biz rules ─│                     │                 │                  │
  │                      │   (status, rights) │                     │                 │                  │
  │                      │                    │                     │                 │                  │
  │                      │── (2) POST /upload-token ───────────────►│                 │                  │
  │                      │◄── {upload_token} ──────────────────────│                 │                  │
  │◄── {upload_token} ──│                    │                     │                 │                  │
  │                      │                    │                     │                 │                  │
  │── (3) POST /upload ──│────────────────────►                     │                 │                  │
  │   X-Upload-Token     │                    │                     │                 │                  │
  │   + file bytes       │                    │── validate token ──│                 │                  │
  │                      │                    │── compute SHA-256 ──│                 │                  │
  │                      │                    │── PUT object ──────►│ (quarantine)    │                  │
  │                      │                    │── INSERT files ─────│ (PostgreSQL)    │                  │
  │                      │                    │── blacklist jti ────│ (Redis)         │                  │
  │                      │                    │── publish ──────────│────────────────►│                  │
  │                      │                    │   (av.scan.request) │                 │                  │
  │◄── (4) 201 {file_id}│────────────────────│                     │                 │                  │
  │                      │                    │                     │                 │                  │
  │── (5) POST /lots/    │                    │                     │                 │                  │
  │   {lotId}/documents/ │                    │                     │                 │                  │
  │   confirm ──────────►│                    │                     │                 │                  │
  │                      │── verify file_id ──►                     │                 │                  │
  │                      │── save in lot model│                     │                 │                  │
  │◄── 200 OK ──────────│                    │                     │                 │                  │
  │                      │                    │                     │                 │                  │
  │                      │                    │                     │                 │── consume ──────►│
  │                      │                    │                     │                 │                  │── scan
  │                      │                    │                     │                 │◄── av.scan.result│
  │                      │                    │◄── consume ─────────│─────────────────│                  │
  │                      │                    │── COPY object ─────►│ (quarantine → target bucket)      │
  │                      │                    │── DELETE object ───►│ (quarantine)    │                  │
  │                      │                    │── UPDATE av_status ─│ (PostgreSQL)    │                  │
  │                      │                    │── publish ──────────│────────────────►│                  │
  │                      │                    │   (FILE_AV_PASSED)  │                 │                  │
```

### 6.1.1. Загрузка файла (M2M — сервис-к-сервису)

Для случаев, когда бизнес-сервис сам генерирует файл (например, DocGen Service создаёт протокол), используется упрощённый поток без upload-token:

```
DocGen Service                DSS API              S3         Kafka
  │                              │                     │                 │
  │── POST /upload ─────────────►│                     │                 │
  │   Authorization: Bearer      │                     │                 │
  │   (service JWT, scope:       │                     │                 │
  │    documents.write)          │                     │                 │
  │   + file + owner_type/id     │                     │                 │
  │                              │── validate JWT ─────│                 │
  │                              │── compute SHA-256 ──│                 │
  │                              │── PUT object ──────►│ (quarantine)    │
  │                              │── INSERT metadata ──│ (PostgreSQL)    │
  │                              │── publish AV task ──│────────────────►│
  │◄── 201 {file_id} ───────────│                     │                 │
```

### 6.2. Скачивание приватного файла (через download-token)

```
Web-клиент          Notice Service         DSS API              Redis             S3
  │                      │                    │                    │                  │
  │── (1) GET /lots/     │                    │                    │                  │
  │   {lotId}/documents/ │                    │                    │                  │
  │   {fileId}/download ►│                    │                    │                  │
  │                      │── check biz rules ─│                    │                  │
  │                      │   (lot status,     │                    │                  │
  │                      │    user is owner?) │                    │                  │
  │                      │                    │                    │                  │
  │                      │── (2) POST         │                    │                  │
  │                      │   /download-token ─►                    │                  │
  │                      │◄── {download_token}│                    │                  │
  │◄── {download_token}─│                    │                    │                  │
  │                      │                    │                    │                  │
  │── (3) GET /download? │                    │                    │                  │
  │   token={token} ─────│───────────────────►│                    │                  │
  │                      │                    │── validate token ──│                  │
  │                      │                    │── blacklist jti ──►│                  │
  │                      │                    │── check av_status ─│                  │
  │                      │                    │── GET cached URL? ─►│                 │
  │                      │                    │◄── miss ───────────│                  │
  │                      │                    │── generate presigned URL ────────────►│
  │                      │                    │◄── presigned URL ──│──────────────────│
  │                      │                    │── cache URL ──────►│ (TTL 4 min)     │
  │◄── (4) 302 redirect ─│────────────────────│                    │                  │
  │                      │                    │                    │                  │
  │── (5) GET presigned URL ──────────────────│────────────────────│─────────────────►│
  │◄── file stream ───────│────────────────────│────────────────────│──────────────────│
```

**Пример: лот ещё не опубликован.**  
На шаге (1) организатор запрашивает документ лота через Notice Service. Notice Service видит, что `lot.status = DRAFT`, проверяет, что `user_id` совпадает с организатором этого лота — и только тогда запрашивает download-token у DSS. Любой другой пользователь получит `403 Forbidden` от Notice Service, и запрос до DSS не дойдёт.

**Пример: участник скачивает свою заявку.**  
Участник запрашивает документ через Application Service. Application Service проверяет, что `application.participant_id == user_id` — и выдаёт download-token. Организатор этого лота тоже может скачать этот документ, но через свой бизнес-сервис, который проверит, что лот принадлежит ему.

### 6.3. Привязка подписи — синхронный путь (HTTP)

Бизнес-сервис вызывает `POST /signatures` по HTTP. DSS проверяет подпись по ГОСТ-хешу из БД.

```
Бизнес-сервис             DSS API            PostgreSQL         Crypto EDS         Kafka
  │                         │                    │                  │                │
  │── POST /{fileId}/       │                    │                  │                │
  │   signatures ──────────►│                    │                  │                │
  │                         │── SELECT           │                  │                │
  │                         │   checksum_gost ──►│                  │                │
  │                         │◄── "7d9e8f..." ────│                  │                │
  │                         │                    │                  │                │
  │                         │── POST /verify ────│─────────────────►│                │
  │                         │   {hash_b64,       │                  │                │
  │                         │    signature_b64}  │                  │── verify ──────│
  │                         │                    │                  │  (без скачива- │
  │                         │                    │                  │   ния файла)   │
  │                         │◄── VerifyResponse ─│──────────────────│                │
  │                         │                    │                  │                │
  │                         │── INSERT ──────────►                  │                │
  │                         │   file_signatures  │                  │                │
  │                         │── publish ─────────│──────────────────│───────────────►│
  │                         │   (SIGNATURE_ADDED)│                  │                │
  │◄── 201 {signature_id,  │                    │                  │                │
  │    verification_status} │                    │                  │                │
```

### 6.4. Привязка подписи — асинхронный путь (Kafka)

Бизнес-сервис публикует сообщение в Kafka. DSS обрабатывает в фоне.

```
Бизнес-сервис     Kafka                       DSS Consumer       PostgreSQL         Crypto EDS
  │                 │                              │                  │                  │
  │── publish ─────►│ documents.signatures.request  │                  │                  │
  │                 │                              │                  │                  │
  │                 │── consume ───────────────────►│                  │                  │
  │                 │                              │── INSERT ────────►│                  │
  │                 │                              │   (status=PENDING)│                  │
  │                 │                              │── SELECT gost ───►│                  │
  │                 │                              │◄── hash ──────────│                  │
  │                 │                              │                  │                  │
  │                 │                              │── publish ───────►│ tasks.crypto_eds │
  │                 │                              │                  │                  │
  │                 │                              │                  │── consume ──────►│
  │                 │                              │                  │                  │── verify
  │                 │                              │                  │◄── result ───────│
  │                 │                              │◄── consume ───────│ results.crypto_eds
  │                 │                              │                  │                  │
  │                 │                              │── UPDATE ────────►│                  │
  │                 │                              │   (status=VALID)  │                  │
  │                 │◄── SIGNATURE_VERIFIED ────────│ documents.events  │                  │
  │                 │                              │                  │                  │
  │◄── consume ────│                              │                  │                  │
  │  (реакция)     │                              │                  │                  │
```

---

## 7. Kafka-топики

| Топик | Направление | Описание | Формат ключа |
|---|---|---|---|
| `documents.av.scan.request` | DSS → AV Worker | Задача на антивирусную проверку | `file_id` |
| `documents.av.scan.result` | AV Worker → DSS | Результат проверки | `file_id` |
| `documents.signatures.request` | Бизнес-сервис → DSS | Асинхронная привязка подписи с проверкой | `file_id` |
| `documents.events` | DSS → * | События жизненного цикла файлов и подписей | `file_id` |
| `audit.document_storage` | DSS → Audit Service | Аудит-события | `correlation_id` |

### 7.1. Формат задачи AV-проверки

```json
{
  "task_id": "uuid",
  "file_id": "file-uuid",
  "storage_key": "documents-quarantine/upload-uuid",
  "bucket": "documents-quarantine",
  "content_type": "application/pdf",
  "size_bytes": 1048576,
  "correlation_id": "uuid",
  "requested_at": "2025-10-20T12:00:00Z"
}
```

### 7.2. Формат результата AV-проверки

```json
{
  "task_id": "uuid",
  "file_id": "file-uuid",
  "status": "CLEAN",
  "engine": "ClamAV 1.3.1",
  "scanned_at": "2025-10-20T12:00:15Z",
  "details": {
    "signatures_checked": 8742156,
    "scan_duration_ms": 340,
    "threats_found": []
  }
}
```

### 7.3. Формат задачи привязки подписи (`documents.signatures.request`)

```json
{
  "task_id": "uuid",
  "file_id": "file-uuid-789",
  "signature_b64": "MIIBxwYJKo...",
  "signature_format": "CADES_BES",
  "hash_algorithm": "gostr3411_2012_256",
  "signer_id": "user-uuid",
  "certificate_serial": "01AB23CD",
  "correlation_id": "corr-uuid",
  "requested_at": "2025-10-20T12:00:00Z"
}
```

### 7.4. Формат события жизненного цикла

```json
{
  "event_type": "FILE_UPLOADED",
  "file_id": "file-uuid",
  "owner_type": "APPLICATION",
  "owner_id": "app-uuid",
  "timestamp": "2025-10-20T12:00:00Z",
  "correlation_id": "uuid",
  "actor_id": "user-uuid",
  "details": {
    "original_name": "charter.pdf",
    "size_bytes": 1048576,
    "checksum_sha256": "a1b2c3...",
    "checksum_gost": "7d9e8f..."
  }
}
```

**Типы событий:** `FILE_UPLOADED`, `FILE_AV_PASSED`, `FILE_AV_FAILED`, `FILE_MOVED_TO_STORAGE`, `FILE_ATTACHED`, `FILE_DETACHED`, `FILE_DELETED`, `FILE_VERSION_CREATED`, `SIGNATURE_ADDED`, `SIGNATURE_VERIFIED`, `SIGNATURE_REVERIFIED`, `PRESIGNED_URL_GENERATED`, `FILE_DOWNLOADED`.

---

## 8. Авторизация и безопасность

### 8.1. Модель доступа

DSS намеренно **не реализует бизнес-логику авторизации**. Он не знает, что такое «лот», «заявка» или «организатор» — и не должен знать. Вся ответственность за проверку прав доступа лежит на бизнес-сервисах.

#### Поверхность API: кто к чему имеет доступ

| Endpoint | Аутентификация | Кто вызывает | Назначение |
|---|---|---|---|
| `POST /upload` | Upload-token (`X-Upload-Token`) | Web-клиент | Загрузка файла |
| `GET /download?token=` | Download-token (query param) | Web-клиент | Скачивание приватного файла |
| `GET /public/{fileId}/download` | Без аутентификации | Web-клиент / CDN | Скачивание публичного файла |
| `POST /upload-token` | Сервисный JWT (`documents.issue_token`) | Бизнес-сервис | Выдача upload-token |
| `POST /download-token` | Сервисный JWT (`documents.issue_token`) | Бизнес-сервис | Выдача download-token |
| `POST /upload` (M2M) | Сервисный JWT (`documents.write`) | Бизнес-сервис | Прямая загрузка (DocGen и т.д.) |
| `GET /documents/{fileId}` | Сервисный JWT (`documents.read`) | Бизнес-сервис | Метаданные файла |
| `GET /documents/by-owner/...` | Сервисный JWT (`documents.read`) | Бизнес-сервис | Список файлов по сущности |
| `DELETE /documents/{fileId}` | Сервисный JWT (`documents.delete`) | Бизнес-сервис | Soft-delete |
| `POST /documents/{fileId}/signatures` | Сервисный JWT (`documents.write`) | Бизнес-сервис | Привязка подписи (sync) |
| `GET /documents/{fileId}/signatures` | Сервисный JWT (`documents.read`) | Бизнес-сервис | Список подписей |
| `POST /.../reverify` | Сервисный JWT (`documents.write`) | Бизнес-сервис | Повторная проверка |
| `POST /documents/{fileId}/versions` | Сервисный JWT (`documents.write`) | Бизнес-сервис | Новая версия файла |
| `GET /documents/{fileId}/versions` | Сервисный JWT (`documents.read`) | Бизнес-сервис | Список версий |
| `POST /documents/{fileId}/presigned-url` | Сервисный JWT (`documents.read`) | Бизнес-сервис | Presigned URL (M2M) |

#### Три зоны доступа

**Зона 1 — Web-клиент (публичная сеть).** Три endpoint'а: upload по токену, download по токену, public без токена. Минимальная поверхность атаки. Web-клиент не может перебирать `file_id`, получать метаданные чужих файлов или узнавать, какие файлы существуют в системе.

**Зона 2 — бизнес-сервисы (внутренняя сеть).** Все остальные endpoint'ы. Доступны только с сервисным JWT, который выдаётся при развёртывании сервиса (не пользователю). Scope JWT определяет доступные операции.

**Зона 3 — Kafka (внутренняя сеть).** Асинхронные операции: привязка подписей (`documents.signatures.request`), AV-проверка, события. Доступ контролируется Kafka ACL.

#### Scopes сервисного JWT

| Scope | Операции |
|---|---|
| `documents.issue_token` | Выдача upload-token и download-token |
| `documents.write` | Загрузка файлов, привязки, подписи, версии |
| `documents.read` | Метаданные, списки, presigned URL |
| `documents.delete` | Soft-delete файлов |

#### Примеры бизнес-логики доступа (на стороне сервисов-потребителей)

| Сценарий | Бизнес-сервис | Логика проверки доступа |
|---|---|---|
| Лот в статусе «Черновик» | Notice Service | Только организатор этого лота (`lot.organizer_id == user_id`) |
| Лот опубликован, документ публичный | — | Download-token не нужен, файл доступен через `/public/` |
| Документы заявки | Application Service | Участник — только свои заявки; организатор — заявки по своим лотам |
| Протокол допуска | Admission Service | Организатор + допущенные участники после публикации протокола |
| Документы возврата | Refund Service | Только владелец заявления и финансовая служба |

#### Что DSS проверяет самостоятельно

- Валидность и одноразовость токенов (upload/download) — через подпись JWT и Redis blacklist.
- Scope сервисного JWT — endpoint доступен только при наличии требуемого scope.
- `av_status = CLEAN` — файлы, не прошедшие антивирус, не отдаются никому.
- Публичные файлы (`visibility = PUBLIC`) — доступны без токена через endpoint `/public/`.
- Rate limiting — по user_id (из токена) и по IP (для публичных).

### 8.2. Безопасность хранения

- Все бакеты используют SSE-S3 (AES-256) шифрование at-rest.
- Presigned URL генерируются с TTL ≤ 5 минут для приватных файлов.
- Для публичных файлов presigned URL с TTL до 1 часа, кешируемые в Redis.
- Доступ к S3 только через DSS (network policy, бакеты не публичные).
- Антивирусная проверка обязательна; файлы с `av_status != CLEAN` недоступны для скачивания (кроме администраторов).

### 8.3. Rate limiting

| Операция | Лимит |
|---|---|
| Upload (на пользователя) | 30 файлов / 5 минут |
| Download presigned URL (на пользователя) | 100 запросов / минуту |
| Публичные документы (на IP) | 200 запросов / минуту |
| API метаданных (на сервис) | 1000 запросов / минуту |

Реализуется через Redis с алгоритмом sliding window.

---

## 9. Публичный доступ и CDN

### 9.1. Архитектура раздачи публичных документов

```
Гость / Участник
      │
      ▼
┌──────────┐     miss    ┌──────────────┐    presigned URL    ┌──────────┐
│  CDN     │ ───────────►│  DSS API     │ ──────────────────► │  S3      │
│ (nginx)  │◄── cache ───│  /public/    │                     │ (public) │
└──────────┘   hit       └──────────────┘                     └──────────┘
```

- Публичные документы (`visibility = PUBLIC`, `av_status = CLEAN`) доступны без авторизации.
- CDN (nginx reverse proxy или cloud CDN) кеширует ответы с `Cache-Control: public, max-age=3600`.
- DSS отдаёт `ETag` и поддерживает `If-None-Match` для условных запросов.
- При обновлении версии документа — инвалидация кеша через CDN API или новый URL.

### 9.2. Мониторинг CDN/SLA

| Метрика | Порог |
|---|---|
| Cache hit ratio | ≥ 80% |
| Ошибки загрузки документов | ≤ 1% |
| Latency P95 (через CDN) | ≤ 500 мс |
| Latency P95 (без CDN, fallback) | ≤ 2 секунды |

---

## 10. Нефункциональные требования

### 10.1. Производительность

| Операция | Требование (P95) |
|---|---|
| Загрузка файла (≤ 20 МБ) | ≤ 2 секунды (без AV) |
| Генерация presigned URL | ≤ 100 мс |
| Антивирусная проверка | ≤ 15 секунд |
| Получение метаданных | ≤ 200 мс |
| Скачивание (redirect) | ≤ 300 мс (до redirect) |
| Привязка подписи + верификация | ≤ 10 секунд |

### 10.2. Надёжность

| Параметр | Требование |
|---|---|
| Success rate загрузки | ≥ 99.5% |
| Success rate скачивания | ≥ 99.9% |
| Повтор AV-проверки при сбое | 3 попытки (5/15/30 секунд) |
| Повтор верификации подписи | 3 попытки (3/10/30 секунд) |
| Дублирование S3 | Обеспечивается провайдером (репликация, erasure coding) |

### 10.3. Доступность

| Параметр | Требование |
|---|---|
| SLA сервиса | ≥ 99.5% |
| RTO | ≤ 30 минут |
| RPO | ≤ 5 минут (метаданные в PostgreSQL) |
| RPO | 0 (файлы в S3 с erasure coding) |

### 10.4. Масштабируемость

- Горизонтальное масштабирование DSS (stateless, за load balancer).
- S3 — масштабируется на стороне провайдера.
- PostgreSQL — read replicas для метаданных.
- AV worker — масштабируется независимо через Kafka consumer groups.
- Целевой объём: ≥ 500 одновременных загрузок/скачиваний.

### 10.5. Хранение и retention

| Класс данных | Срок хранения | Tier |
|---|---|---|
| Файлы заявок и протоколов | ≥ 5 лет | Hot (1 год) → Cold (далее) |
| Электронные подписи | ≥ 5 лет | PostgreSQL (в составе бэкапов метаданных) |
| Публичные документы извещений | ≥ 3 года | Hot (6 мес) → Cold |
| Карантинные файлы (AV fail) | 90 дней | Hot, затем удаление |
| Метаданные (PostgreSQL) | ≥ 5 лет | Полные бэкапы ежедневно |
| Логи аудита | ≥ 5 лет | Через Kafka → Audit Service |

---

## 11. Prometheus-метрики

| Метрика | Labels | Тип |
|---|---|---|
| `dss_upload_requests_total` | `status`, `content_type`, `owner_type` | Counter |
| `dss_upload_token_issued_total` | `owner_type`, `status` | Counter |
| `dss_upload_token_validation_total` | `status` (valid/expired/blacklisted/invalid_sig) | Counter |
| `dss_upload_duration_seconds` | `content_type`, `size_bucket` | Histogram |
| `dss_upload_size_bytes` | `content_type`, `owner_type` | Histogram |
| `dss_download_requests_total` | `status`, `visibility` | Counter |
| `dss_download_token_issued_total` | `status` | Counter |
| `dss_download_token_validation_total` | `status` (valid/expired/blacklisted/invalid_sig) | Counter |
| `dss_presigned_url_duration_seconds` | `visibility` | Histogram |
| `dss_av_scan_duration_seconds` | `status`, `engine` | Histogram |
| `dss_av_scan_results_total` | `status` (CLEAN/INFECTED/ERROR) | Counter |
| `dss_signature_verify_total` | `status`, `format` | Counter |
| `dss_signature_verify_duration_seconds` | `format` | Histogram |
| `dss_s3_operations_total` | `operation`, `bucket`, `status` | Counter |
| `dss_s3_operation_duration_seconds` | `operation`, `bucket` | Histogram |
| `dss_quarantine_files_count` | — | Gauge |

---

## 12. Структура проекта

```
document-storage-service/
├── app/
│   ├── main.py                           # Точка входа FastAPI
│   ├── config.py                         # Pydantic Settings
│   ├── dependencies.py                   # DI container
│   │
│   ├── api/v2/
│   │   ├── routes_upload.py              # POST /upload
│   │   ├── routes_upload_token.py        # POST /upload-token (выдача токенов бизнес-сервисам)
│   │   ├── routes_download.py            # GET /download (по download-token), presigned URL (M2M)
│   │   ├── routes_download_token.py      # POST /download-token (выдача токенов бизнес-сервисам)
│   │   ├── routes_metadata.py            # GET/DELETE метаданные, привязки
│   │   ├── routes_public.py              # GET /public/ (без авторизации)
│   │   ├── routes_signatures.py          # POST/GET подписи
│   │   └── routes_versions.py            # POST/GET версии
│   │
│   ├── domain/
│   │   ├── models.py                     # Pydantic-модели (запросы, ответы, события)
│   │   ├── file_service.py               # Бизнес-логика загрузки/скачивания
│   │   ├── hash_calculator.py            # Streaming-вычисление SHA-256 + ГОСТ 34.11-2012 (pygost)
│   │   ├── upload_token_service.py        # Генерация и валидация upload-token (JWT)
│   │   ├── download_token_service.py     # Генерация и валидация download-token (JWT)
│   │   ├── signature_service.py           # Хранение подписей в БД и проверка через Crypto EDS
│   │   ├── av_service.py                 # Логика антивирусной проверки
│   │   ├── version_service.py            # Версионирование
│   │   └── validators.py                 # Валидация форматов, размеров
│   │
│   ├── storage/
│   │   ├── s3_client.py                  # Async S3 клиент (aiobotocore)
│   │   ├── metadata_repository.py         # SQLAlchemy async repository
│   │   ├── signature_repository.py        # Repository для подписей
│   │   └── cache_client.py               # Redis client
│   │
│   ├── infrastructure/
│   │   ├── auth.py                       # JWT / Bearer auth + access check
│   │   ├── correlation.py                # X-Correlation-Id middleware
│   │   ├── logging.py                    # structlog JSON
│   │   ├── metrics.py                    # Prometheus
│   │   ├── audit.py                      # Kafka audit producer
│   │   ├── events.py                     # Kafka event producer (lifecycle)
│   │   ├── av_consumer.py                # Kafka consumer для AV результатов
│   │   ├── av_task_producer.py           # Kafka producer для AV задач
│   │   ├── signature_consumer.py         # Kafka consumer для async-подписей (documents.signatures.request)
│   │   ├── crypto_eds_client.py          # HTTP-клиент к Crypto EDS
│   │   └── rate_limiter.py               # Redis-based rate limiting
│   │
│   └── workers/
│       └── av_worker.py                  # ClamAV worker (clamd socket)
│
├── migrations/                           # Alembic миграции
│   ├── versions/
│   └── env.py
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
│
├── docs/
│   └── openapi.yaml                      # OpenAPI 3.1 spec
│
├── Dockerfile
├── docker-compose.yml
├── alembic.ini
├── justfile
├── pyproject.toml
└── .env.example
```

---

## 13. Docker Compose (dev-окружение)

| Сервис | Порт | Описание |
|---|---|---|
| `app` | 8001 | Document Storage Service API |
| `av-worker` | — | ClamAV worker (Kafka consumer) |
| `postgres` | 5432 | PostgreSQL 16 |
| `redis` | 6379 | Redis 7 |
| `minio` | 9000, 9001 | MinIO S3 (dev/test — замена продуктивного S3-провайдера) |
| `clamav` | 3310 | ClamAV daemon |
| `kafka` | 29092 | Apache Kafka (shared с другими сервисами) |

---

## 14. Интеграции

| Система | Протокол | Назначение | Таймаут | Fallback |
|---|---|---|---|---|
| **S3-хранилище** | S3 API (HTTP) | Хранение файлов документов | 30 сек (upload), 5 сек (presigned) | Retry 3x, alert P1 |
| **PostgreSQL** | TCP | Метаданные файлов | 3 сек | Read replica, alert P1 |
| **Redis** | TCP | Кеш presigned URL, rate limiting, blacklist одноразовых токенов | 500 мс | Bypass кеша; при недоступности blacklist — отклонение токенов (fail-closed) |
| **Kafka** | TCP | AV-задачи, события, аудит | 5 сек | Retry с экспоненциальной задержкой |
| **ClamAV** | TCP/Unix socket | Антивирус | 30 сек | Retry 3x, карантин продлевается |
| **Crypto EDS** | HTTP (REST) | Проверка подписей | 10 сек | Retry 3x, статус `PENDING` |

---

## 15. Риски и митигация

| ID | Риск | Вероятность | Влияние | Митигация |
|---|---|---|---|---|
| R-01 | Перегрузка S3 при массовой загрузке | Средняя | Высокое | Rate limiting, очередь загрузок, масштабирование на стороне провайдера |
| R-02 | Потеря файла при переносе из карантина | Низкая | Критическое | Атомарная операция copy+delete, проверка checksum после копирования |
| R-03 | ClamAV недоступен | Средняя | Среднее | Файлы остаются в карантине, retry через Kafka, alert P2 |
| R-04 | Рост объёма хранилища | Высокая | Среднее | Lifecycle policies (hot → cold), мониторинг ёмкости, алерты при 80% |
| R-05 | Утечка presigned URL | Низкая | Среднее | Короткий TTL (5 мин), аудит генерации URL, IP-привязка (опционально) |
| R-06 | Несогласованность метаданных и S3 | Низкая | Высокое | Reconciliation job (ежедневно), checksum verification |
| R-07 | Concurrent создание версий одного файла | Средняя | Среднее | Проверка `is_latest = true` в транзакции, optimistic locking |

---

## 16. Миграция и развёртывание

### 16.1. Порядок развёртывания

1. Создать бакеты в S3-хранилище провайдера (или развернуть MinIO для dev) и применить политики.
2. Развернуть PostgreSQL и применить миграции Alembic.
3. Создать топики Kafka.
4. Развернуть ClamAV daemon.
5. Развернуть DSS API + AV Worker + Session Cleanup.
6. Настроить CDN / nginx proxy для публичных документов.
7. Обновить сервисы-потребители для перехода на DSS API.

### 16.2. Конфигурация (.env)

```env
# S3 (провайдер-агностик: Timeweb S3 / AWS S3 / MinIO для dev)
S3_ENDPOINT=http://minio:9000                    # dev: MinIO; prod: https://s3.timeweb.cloud
S3_ACCESS_KEY=minioadmin                         # dev credentials
S3_SECRET_KEY=minioadmin                         # dev credentials
S3_REGION=us-east-1
S3_BUCKET_PRIVATE=documents-private
S3_BUCKET_PUBLIC=documents-public
S3_BUCKET_QUARANTINE=documents-quarantine

# PostgreSQL
DATABASE_URL=postgresql+asyncpg://dss:password@postgres:5432/document_storage

# Redis
REDIS_URL=redis://redis:6379/0

# Kafka
KAFKA_BOOTSTRAP=kafka:9092
KAFKA_TOPIC_AV_REQUEST=documents.av.scan.request
KAFKA_TOPIC_AV_RESULT=documents.av.scan.result
KAFKA_TOPIC_SIGNATURES_REQUEST=documents.signatures.request
KAFKA_TOPIC_EVENTS=documents.events
KAFKA_TOPIC_AUDIT=audit.document_storage

# ClamAV
CLAMAV_HOST=clamav
CLAMAV_PORT=3310

# Crypto EDS
CRYPTO_EDS_URL=http://crypto-eds:8000
CRYPTO_EDS_API_KEY=...

# IAM
ACCESS_CONTROL_URL=http://iam:8000

# Uploads
MAX_FILE_SIZE_MB=20
ALLOWED_CONTENT_TYPES=application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,image/jpeg,image/png
PRESIGNED_URL_TTL_SECONDS=300
PUBLIC_PRESIGNED_URL_TTL_SECONDS=3600

# Upload-token
UPLOAD_TOKEN_SECRET=<random-256-bit-secret>
UPLOAD_TOKEN_TTL_SECONDS=600
UPLOAD_TOKEN_ALGORITHM=HS256

# Download-token
DOWNLOAD_TOKEN_SECRET=<random-256-bit-secret>
DOWNLOAD_TOKEN_MAX_TTL_SECONDS=600
DOWNLOAD_TOKEN_ALGORITHM=HS256

# Rate limits
RATE_LIMIT_UPLOAD_PER_USER=30/5m
RATE_LIMIT_DOWNLOAD_PER_USER=100/1m
RATE_LIMIT_PUBLIC_PER_IP=200/1m
```

---

## 17. Связанные документы

**Use Cases:**
- [UC-020] Подача заявки на участие в торгах по лоту
- [UC-003] Изменение извещения и лотов
- [UC-008] Формирование протокола допуска участников
- [UC-015] Формирование итогового протокола
- [UC-018] Допуск участника к участию в торгах
- [UC-022] Приостановка торгов по лоту
- [UC-012] Возврат средств
- [UC-004] Просмотр публичного каталога

**Технические документы:**
- [DOC-010] Хранилище документов и версия файлов
- [DOC-021] Руководство по работе с документами и антивирусной проверкой
- [SEC-020] Требования к электронной подписи
- README_CRYPTO.md — Crypto EDS (проверка подписей)

**Регуляторные:**
- 44-ФЗ / 223-ФЗ — требования к хранению документов
- 63-ФЗ — электронная подпись
- 152-ФЗ — персональные данные

---

## 18. История изменений

| Версия | Дата | Автор | Описание |
|---|---|---|---|
| 2.3 | 2025-10-20 | Команда инфраструктуры | Удалена таблица file_attachments — owner_type/owner_id перенесены в files. Два таблицы: files + file_signatures. Удалены endpoints attach/detach |
| 2.2 | 2025-10-20 | Команда инфраструктуры | Удалено поле category — классификация документов является ответственностью бизнес-сервисов |
| 2.1 | 2025-10-20 | Команда инфраструктуры | Детальное описание версионирования: подписи не наследуются, привязки переносятся, аудитный след сохраняется |
| 2.0 | 2025-10-20 | Команда инфраструктуры | Явное разделение API на три зоны: Web-клиент (токены + public), M2M (сервисный JWT), Kafka. Метаданные/привязки/подписи — только M2M |
| 1.9 | 2025-10-20 | Команда инфраструктуры | Удалено поле slot — маппинг «документ → логическая позиция» является ответственностью бизнес-сервисов |
| 1.8 | 2025-10-20 | Команда инфраструктуры | Удалено поле etag из таблицы files — контроль целостности через checksum_sha256 |
| 1.7 | 2025-10-20 | Команда инфраструктуры | S3-провайдер агностик: MinIO только для dev/test, прод — Timeweb S3 / иной провайдер |
| 1.6 | 2025-10-20 | Команда инфраструктуры | Выбор sync/async проверки подписи — ответственность бизнес-сервиса (HTTP vs Kafka), убран verify_mode из API |
| 1.5 | 2025-10-20 | Команда инфраструктуры | Dual hash при загрузке (SHA-256 + ГОСТ через pygost), verify_mode для подписей (sync_hash/sync_file_url/async/none) |
| 1.4 | 2025-10-20 | Команда инфраструктуры | ЭЦП хранятся в PostgreSQL (base64), удалён бакет documents-signatures |
| 1.3 | 2025-10-20 | Команда инфраструктуры | Download-token: авторизация скачивания делегирована бизнес-сервисам, удалена зависимость от IAM |
| 1.2 | 2025-10-20 | Команда инфраструктуры | Удалён chunked upload (лимит 20 МБ), упрощена модель данных и API |
| 1.1 | 2025-10-20 | Команда инфраструктуры | Переход на двухфазную загрузку (claim check pattern), upload-token, обновлены диаграммы потоков |
| 1.0 | 2025-10-20 | Команда инфраструктуры | Первая версия архитектурного проекта |
