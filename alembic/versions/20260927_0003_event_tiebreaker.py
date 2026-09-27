"""add deterministic event tie-breaker and remove obsolete replica table

Revision ID: 20260927_0003
Revises: 20260927_0002
"""
import json
import uuid

from alembic import op
import sqlalchemy as sa


revision = "20260927_0003"
down_revision = "20260927_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("orders", sa.Column("last_event_id", sa.String(length=36), nullable=True))

    bind = op.get_bind()
    for row in bind.execute(
        sa.text(
            "SELECT order_id, item_name, price, version, is_deleted "
            "FROM order_replicas ORDER BY order_id"
        )
    ).mappings():
        order_id = int(row["order_id"])
        record_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"event-driven-system:legacy-replica:order:{order_id}",
            )
        )
        origin_node = "legacy-replica"
        updated_by_node = origin_node
        item_name = row["item_name"] or ""
        price = int(row["price"] or 0)
        version = max(int(row["version"]), 1)
        is_deleted = bool(row["is_deleted"])
        event_type = "ORDER_DELETED" if is_deleted else "ORDER_CREATED"
        payload = {"item_name": item_name, "price": price}
        event_seed = json.dumps(
            {
                "aggregate_id": record_id,
                "version": version,
                "type": event_type,
                "payload": payload,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        event_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"event-driven-system:{event_seed}")
        )

        bind.execute(
            sa.text(
                "INSERT INTO orders "
                "(record_id, origin_node, updated_by_node, item_name, price, version, "
                "is_deleted, last_event_id) "
                "VALUES (:record_id, :origin_node, :updated_by_node, :item_name, :price, "
                ":version, :is_deleted, :last_event_id)"
            ),
            {
                "record_id": record_id,
                "origin_node": origin_node,
                "updated_by_node": updated_by_node,
                "item_name": item_name,
                "price": price,
                "version": version,
                "is_deleted": is_deleted,
                "last_event_id": event_id,
            },
        )
        bind.execute(
            sa.text(
                "INSERT INTO outbox "
                "(event_id, aggregate_id, origin_node, updated_by_node, version, "
                "event_type, payload, is_sent) "
                "VALUES (:event_id, :aggregate_id, :origin_node, :updated_by_node, "
                ":version, :event_type, :payload, false)"
            ),
            {
                "event_id": event_id,
                "aggregate_id": record_id,
                "origin_node": origin_node,
                "updated_by_node": updated_by_node,
                "version": version,
                "event_type": event_type,
                "payload": json.dumps(payload),
            },
        )

    op.drop_table("order_replicas")


def downgrade() -> None:
    op.create_table(
        "order_replicas",
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("item_name", sa.String(length=100), nullable=True),
        sa.Column("price", sa.Integer(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.PrimaryKeyConstraint("order_id"),
    )
    op.drop_column("orders", "last_event_id")
