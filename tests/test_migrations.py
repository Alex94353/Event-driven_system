import uuid

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text


def test_migration_backfills_existing_order_and_outbox(tmp_path, monkeypatch):
    database_url = f"sqlite:///{tmp_path / 'legacy.db'}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("NODE_ID", "node_b")
    config = Config("alembic.ini")

    command.upgrade(config, "20260927_0001")
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO orders (id, item_name, price, version) VALUES (7, 'old book', 9, 2)")
        )
        connection.execute(
            text(
                "INSERT INTO outbox "
                "(id, event_id, aggregate_id, version, event_type, payload, is_sent) "
                "VALUES (1, 'legacy-event', '7', 2, 'ORDER_UPDATED', "
                "'{\"order_id\": 7, \"item_name\": \"old book\", \"price\": 9}', 0)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO order_replicas "
                "(order_id, item_name, price, version, is_deleted) "
                "VALUES (23, 'replicated legacy item', 31, 4, 0)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO order_replicas "
                "(order_id, item_name, price, version, is_deleted) "
                "VALUES (24, 'deleted legacy item', 0, 5, 1)"
            )
        )

    command.upgrade(config, "head")
    with engine.connect() as connection:
        order = connection.execute(
            text("SELECT record_id, origin_node, updated_by_node, is_deleted FROM orders WHERE id = 7")
        ).mappings().one()
        event = connection.execute(
            text("SELECT aggregate_id, origin_node, updated_by_node FROM outbox WHERE id = 1")
        ).mappings().one()
        replica = connection.execute(
            text(
                "SELECT record_id, origin_node, updated_by_node, item_name, price, version, "
                "is_deleted, last_event_id FROM orders "
                "WHERE origin_node = 'legacy-replica' AND item_name = 'replicated legacy item'"
            )
        ).mappings().one()
        replicated_outbox = connection.execute(
            text(
                "SELECT event_id, aggregate_id, origin_node, event_type, is_sent "
                "FROM outbox WHERE aggregate_id = :record_id"
            ),
            {"record_id": replica["record_id"]},
        ).mappings().one()
        deleted_replica = connection.execute(
            text(
                "SELECT record_id, is_deleted, last_event_id FROM orders "
                "WHERE origin_node = 'legacy-replica' AND item_name = 'deleted legacy item'"
            )
        ).mappings().one()
        deleted_outbox = connection.execute(
            text(
                "SELECT event_id, event_type FROM outbox WHERE aggregate_id = :record_id"
            ),
            {"record_id": deleted_replica["record_id"]},
        ).mappings().one()

    expected_id = str(
        uuid.uuid5(uuid.NAMESPACE_URL, "event-driven-system:node_b:order:7")
    )
    assert order["record_id"] == expected_id
    assert order["origin_node"] == "node_b"
    assert order["updated_by_node"] == "node_b"
    assert not bool(order["is_deleted"])
    assert event["aggregate_id"] == expected_id
    assert event["origin_node"] == "node_b"
    assert event["updated_by_node"] == "node_b"
    assert replica["record_id"] == str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "event-driven-system:legacy-replica:order:23",
        )
    )
    assert replica["origin_node"] == "legacy-replica"
    assert replica["updated_by_node"] == "legacy-replica"
    assert replica["item_name"] == "replicated legacy item"
    assert replica["price"] == 31
    assert replica["version"] == 4
    assert bool(replica["is_deleted"]) is False
    assert replica["last_event_id"] == replicated_outbox["event_id"]
    assert replicated_outbox["aggregate_id"] == replica["record_id"]
    assert replicated_outbox["origin_node"] == "legacy-replica"
    assert replicated_outbox["event_type"] == "ORDER_CREATED"
    assert bool(replicated_outbox["is_sent"]) is False
    assert bool(deleted_replica["is_deleted"]) is True
    assert deleted_replica["last_event_id"] == deleted_outbox["event_id"]
    assert deleted_outbox["event_type"] == "ORDER_DELETED"
    engine.dispose()
