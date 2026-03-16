import uuid as uuid_mod
from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey,
    Index, Integer, String, Uuid, text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column
from sqlalchemy.sql import func


class Base(MappedAsDataclass, DeclarativeBase):
    pass


class File(Base):
    __tablename__ = "files"

    # Required fields (no defaults) — must come first in dataclass ordering
    original_name: Mapped[str] = mapped_column(String(500))
    storage_key: Mapped[str] = mapped_column(String(1000))
    bucket: Mapped[str] = mapped_column(String(100))
    content_type: Mapped[str] = mapped_column(String(255))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    checksum_sha256: Mapped[str] = mapped_column(String(64))
    checksum_gost: Mapped[str] = mapped_column(String(64))
    owner_type: Mapped[str] = mapped_column(String(50))
    owner_id: Mapped[uuid_mod.UUID] = mapped_column(Uuid)
    uploaded_by: Mapped[uuid_mod.UUID] = mapped_column(Uuid)

    # Fields with defaults
    id: Mapped[uuid_mod.UUID] = mapped_column(
        Uuid, primary_key=True, default_factory=uuid_mod.uuid4, init=False
    )
    version: Mapped[int] = mapped_column(Integer, default=1)
    s3_version_id: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)
    is_latest: Mapped[bool] = mapped_column(Boolean, default=True)
    previous_version_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        Uuid, ForeignKey("files.id"), nullable=True, default=None
    )
    visibility: Mapped[str] = mapped_column(String(20), default="PRIVATE")
    av_status: Mapped[str] = mapped_column(String(20), default="PENDING")
    av_scanned_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    av_engine: Mapped[str | None] = mapped_column(String(100), nullable=True, default=None)
    av_report: Mapped[dict | None] = mapped_column(JSONB, nullable=True, default=None)
    uploaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), init=False
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    correlation_id: Mapped[uuid_mod.UUID | None] = mapped_column(Uuid, nullable=True, default=None)
    metadata_: Mapped[dict | None] = mapped_column(
        JSONB, name="metadata", key="metadata_", nullable=True, default=None
    )

    __table_args__ = (
        CheckConstraint("visibility IN ('PRIVATE', 'PUBLIC')", name="chk_visibility"),
        CheckConstraint(
            "av_status IN ('PENDING', 'SCANNING', 'CLEAN', 'INFECTED', 'ERROR')",
            name="chk_av_status",
        ),
        Index("idx_files_storage_key", "storage_key"),
        Index(
            "idx_files_owner", "owner_type", "owner_id",
            postgresql_where=text("deleted_at IS NULL"),
        ),
        Index("idx_files_uploaded_by", "uploaded_by"),
        Index(
            "idx_files_av_status", "av_status",
            postgresql_where=text("av_status != 'CLEAN'"),
        ),
        Index(
            "idx_files_visibility", "visibility",
            postgresql_where=text("visibility = 'PUBLIC'"),
        ),
    )
