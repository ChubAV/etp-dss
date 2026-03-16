# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DSS (Document Storage Service) is a microservice for the electronic trading platform (ETP) that handles file storage, management, and delivery. It is the single entry point for all file operations across the platform, integrating with S3-compatible storage, antivirus scanning (ClamAV), digital signature verification, and CDN delivery.

The project is in early development — the architectural specification lives in `docs/tz_dss.md` (in Russian).

## Tech Stack

- **Language:** Python 3.12
- **Framework:** FastAPI (async)
- **Database:** PostgreSQL 16 via SQLAlchemy 2.0 async + asyncpg, migrations via Alembic
- **Object Storage:** S3-compatible (Timeweb S3 in prod, MinIO for dev/test) via aiobotocore
- **Cache/Rate Limiting:** Redis 7
- **Message Queue:** Apache Kafka (AV scan tasks, audit events)
- **Antivirus:** ClamAV via unix socket/TCP
- **GOST Hashing:** pygost (GOST 34.11-2012 Streebog)
- **Logging:** structlog (JSON format)
- **Metrics:** prometheus-client
- **Package Manager:** uv (see pyproject.toml)

## Commands

```bash
# Run the application
uv run python main.py

# Install dependencies
uv sync
```

## Architecture

Three-layer architecture (see `docs/tz_dss.md` section 3 for full details):

```
API Layer        → FastAPI routes (upload, upload-token, download, metadata, public)
Domain Layer     → file_service, signature_service, models
Storage Layer    → s3_client, metadata_repository, cache_client
Infrastructure   → Kafka, ClamAV, Redis, PostgreSQL, Prometheus
```

### Key Integration Pattern: Two-Phase Upload (Claim Check)

Business services (Notice Service, Application Service, etc.) do NOT proxy file bytes. Instead:
1. Business service requests an upload-token from DSS (JWT, 10min TTL, one-time use)
2. Web client uploads file directly to DSS with the token
3. Client confirms file_id back to the business service

For M2M calls (e.g., DocGen Service), a service JWT with `documents.write` scope is used instead of upload-tokens.

### S3 Bucket Layout

- `documents-private/` — private files (applications, protocols, decisions), SSE-S3 encrypted, versioned
- `documents-public/` — public notice documents for catalog, served via presigned URL / CDN
- `documents-quarantine/` — temporary holding until AV scan completes, auto-deleted after 24h

### Data Model

Two main PostgreSQL tables:
- `files` — file metadata, S3 keys, checksums (SHA-256 + GOST), owner binding, AV status, versioning
- `file_signatures` — digital signatures (CAdES/PKCS#7 as base64 in DB, not S3), verification results

### Auth Model

- **Upload-token** (`X-Upload-Token`) — web client file uploads only
- **Download-token** (query param) — web client file downloads only
- **Service JWT** (`Authorization: Bearer`) — M2M calls for metadata, tokens, signatures, etc.
- **No auth** — public file downloads (`/public/{fileId}/download`)

DSS does NOT perform business-level authorization — that is the responsibility of the calling business service.

### API Base Path

`/api/v2/documents`

### Important Implementation Details

- File checksums (SHA-256 and GOST Streebog) must be computed in a single streaming pass without loading the full file into memory
- Upload tokens are one-time use — after consumption, `jti` is blacklisted in Redis for remaining TTL
- Files go to quarantine bucket first, moved to target bucket only after AV scan passes
- Digital signatures are stored in PostgreSQL (not S3) due to small size (2-10 KB base64)
- File format whitelist: PDF, DOCX, XLSX, JPG, PNG
- Logging format must match Crypto EDS service (structlog JSON)
