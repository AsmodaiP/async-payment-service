"""Create payments and transactional outbox tables.

Revision ID: 0001_initial
Revises:
Create Date: 2026-08-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_fingerprint", sa.CHAR(length=64), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "webhook_attempts",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("webhook_lock_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("webhook_locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_last_error", sa.String(length=2000), nullable=True),
        sa.CheckConstraint("amount > 0", name=op.f("ck_payments_amount_positive")),
        sa.CheckConstraint(
            "currency IN ('RUB', 'USD', 'EUR')",
            name=op.f("ck_payments_currency_valid"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')",
            name=op.f("ck_payments_status_valid"),
        ),
        sa.CheckConstraint(
            "webhook_attempts BETWEEN 0 AND 3",
            name=op.f("ck_payments_webhook_attempts_range"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_payments"),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )

    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column(
            "schema_version",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "publish_attempts",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("last_error", sa.String(length=2000), nullable=True),
        sa.Column("locked_by", sa.String(length=255), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "publish_attempts >= 0",
            name=op.f("ck_outbox_publish_attempts_nonnegative"),
        ),
        sa.CheckConstraint(
            "schema_version > 0",
            name=op.f("ck_outbox_schema_version_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["aggregate_id"],
            ["payments.id"],
            name="fk_outbox_aggregate_id_payments",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
        sa.UniqueConstraint(
            "aggregate_id",
            "event_type",
            name="uq_outbox_aggregate_id_event_type",
        ),
    )
    op.create_index(
        "ix_outbox_unpublished_polling",
        "outbox",
        ["available_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_unpublished_polling",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox")
    op.drop_table("payments")
