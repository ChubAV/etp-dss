run:
    uv run uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload

test:
    uv run python -m pytest -v

test-unit:
    uv run python -m pytest tests/unit -v

test-integration:
    uv run python -m pytest tests/integration -v -m integration

test-one TEST:
    uv run python -m pytest {{TEST}} -v

lint:
    uv run ruff check .
    uv run ruff format --check .

fix:
    uv run ruff check --fix .
    uv run ruff format .

migration MSG:
    uv run alembic revision --autogenerate -m "{{MSG}}"

migrate:
    uv run alembic upgrade head

up:
    docker compose up -d

down:
    docker compose down

rebuild:
    docker compose up -d --build
