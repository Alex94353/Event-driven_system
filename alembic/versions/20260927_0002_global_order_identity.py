"""add global order identity and provenance

Revision ID: 20260927_0002
Revises: 20260927_0001
"""
import os
import uuid

from alembic import op
import sqlalchemy as sa


revision = "20260927_0002"
down_revision = "20260927_0001"
branch_labels = None
depends_on = None


def _legacy_record_id(node_id: str, order_id: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"event-driven-system:{node_id}:order:{order_id}"))


def upgrade() -> None:
    op.add_column("orders", sa.Column("record_id", sa.String(length=36), nullable=True))
    op.add_column("orders", sa.Column("origin_node", sa.String(length=100), nullable=True))
    op.add_column("orders", sa.Column("updated_by_node", sa.String(length=100), nullable=True))
    op.add_column(
        "orders",
        sa.Column("is_deleted", sa.Boolean(), nullable=True, server_default=sa.false()),
    )
    op.add_column("outbox", sa.Column("origin_node", sa.String(length=100), nullable=True))
    op.add_column("outbox", sa.Column("updated_by_node", sa.String(length=100), nullable=True))

    bind = op.get_bind()
    node_id = os.getenv("NODE_ID", "legacy")
    record_ids: dict[int, str] = {}
    for row in bind.execute(sa.text("SELECT id FROM orders")).mappings():
        order_id = int(row["id"])
        record_id = _legacy_record_id(node_id, order_id)
        record_ids[order_id] = record_id
        bind.execute(
            sa.text(
                "UPDATE orders SET record_id = :record_id, origin_node = :node_id, "
                "updated_by_node = :node_id, is_deleted = false WHERE id = :order_id"
            ),
            {"record_id": record_id, "node_id": node_id, "order_id": order_id},
        )

    for row in bind.execute(sa.text("SELECT id, aggregate_id, payload FROM outbox")).mappings():
        payload = row["payload"] or {}
        legacy_id = payload.get("order_id") if isinstance(payload, dict) else None
        try:
            order_id = int(legacy_id if legacy_id is not None else row["aggregate_id"])
        except (TypeError, ValueError):
            order_id = -int(row["id"])
        record_id = record_ids.get(order_id, _legacy_record_id(node_id, order_id))
        bind.execute(
            sa.text(
                "UPDATE outbox SET aggregate_id = :record_id, origin_node = :node_id, "
                "updated_by_node = :node_id WHERE id = :outbox_id"
            ),
            {"record_id": record_id, "node_id": node_id, "outbox_id": row["id"]},
        )

    with op.batch_alter_table("orders") as batch_op:
        batch_op.alter_column("record_id", existing_type=sa.String(length=36), nullable=False)
        batch_op.alter_column("origin_node", existing_type=sa.String(length=100), nullable=False)
        batch_op.alter_column("updated_by_node", existing_type=sa.String(length=100), nullable=False)
        batch_op.alter_column(
            "is_deleted",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        )
        batch_op.create_unique_constraint("uq_orders_record_id", ["record_id"])

    with op.batch_alter_table("outbox") as batch_op:
        batch_op.alter_column("origin_node", existing_type=sa.String(length=100), nullable=False)
        batch_op.alter_column("updated_by_node", existing_type=sa.String(length=100), nullable=False)


def downgrade() -> None:
    with op.batch_alter_table("outbox") as batch_op:
        batch_op.drop_column("updated_by_node")
        batch_op.drop_column("origin_node")

    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_constraint("uq_orders_record_id", type_="unique")
        batch_op.drop_column("is_deleted")
        batch_op.drop_column("updated_by_node")
        batch_op.drop_column("origin_node")
        batch_op.drop_column("record_id")
