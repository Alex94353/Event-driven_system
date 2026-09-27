import asyncio
import json

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import main
import receiver
from models import Base, Inbox, Order, Outbox
from receiver import process_message
from worker import publish_pending_events

RECORD_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


class FakePublisher:
    def __init__(self):
        self.fail = True
        self.published = []

    async def publish(self, message, routing_key, mandatory=False):
        if self.fail:
            self.fail = False
            raise RuntimeError("broker unavailable")
        self.published.append((message, routing_key, mandatory))


class FakeMessage:
    def __init__(self, body, headers=None):
        self.body = json.dumps(body).encode()
        self.headers = headers or {}
        self.content_type = "application/json"
        self.correlation_id = None
        self.message_id = None
        self.channel = FakeChannel()
        self.acked = 0
        self.nacked = []
        self.rejected = []

    async def ack(self):
        self.acked += 1

    async def nack(self, requeue=False):
        self.nacked.append(requeue)

    async def reject(self, requeue=False):
        self.rejected.append(requeue)


class FailingSession:
    def begin(self):
        raise RuntimeError("database unavailable")

    def rollback(self):
        pass

    def close(self):
        pass


class FakeChannel:
    def __init__(self):
        self.default_exchange = FakeDefaultExchange()

    async def get_exchange(self, name, ensure=True):
        assert name == ""
        assert ensure is False
        return self.default_exchange


class FakeDefaultExchange:
    def __init__(self):
        self.published = []

    async def publish(self, message, routing_key, mandatory=False):
        self.published.append((message, routing_key, mandatory))


class FakeQueue:
    def __init__(self):
        self.bindings = []

    async def bind(self, exchange, routing_key):
        self.bindings.append((exchange, routing_key))


class FakeTopologyChannel:
    def __init__(self):
        self.queues = {}
        self.queue_arguments = {}
        self.qos = None
        self.exchange = object()

    async def declare_exchange(self, name, exchange_type, durable):
        assert name == receiver.RABBITMQ_EXCHANGE
        assert exchange_type == receiver.aio_pika.ExchangeType.TOPIC
        assert durable is True
        return self.exchange

    async def declare_queue(self, name, durable, arguments=None):
        self.queues[name] = FakeQueue()
        self.queue_arguments[name] = arguments
        return self.queues[name]

    async def set_qos(self, prefetch_count):
        self.qos = prefetch_count


def event(event_id, version, event_type="ORDER_UPDATED", item_name=None):
    return {
        "event_id": event_id,
        "aggregate_id": RECORD_ID,
        "origin_node": "node_a",
        "updated_by_node": "node_a",
        "version": version,
        "type": event_type,
        "payload": {"item_name": item_name or f"v{version}", "price": version},
    }


def test_publish_failure_leaves_outbox_unsent_and_retry_sends(session_factory):
    db = session_factory()
    db.add(Outbox(
        event_id="event-1", aggregate_id="1", version=1,
        event_type="ORDER_CREATED", payload={"item_name": "book", "price": 10},
    ))
    db.commit()
    publisher = FakePublisher()

    with pytest.raises(RuntimeError):
        asyncio.run(publish_pending_events(db, publisher))
    assert db.query(Outbox).one().is_sent is False

    asyncio.run(publish_pending_events(db, publisher))
    assert db.query(Outbox).one().is_sent is True
    assert {route for _, route, _ in publisher.published} == {"node.node_b", "node.node_c"}
    assert all(mandatory for _, _, mandatory in publisher.published)


def test_receiver_declares_durable_bounded_retry_topology():
    channel = FakeTopologyChannel()

    queue = asyncio.run(receiver.declare_receiver_topology(channel))

    assert queue is channel.queues[receiver.QUEUE_NAME]
    assert queue.bindings == [(channel.exchange, f"node.{receiver.NODE_ID}")]
    assert channel.queues[receiver.DEAD_QUEUE_NAME]
    assert channel.queue_arguments[receiver.RETRY_QUEUE_NAME] == {
        "x-queue-type": "quorum",
        "x-message-ttl": receiver.RETRY_DELAY_MS,
        "x-dead-letter-exchange": receiver.RABBITMQ_EXCHANGE,
        "x-dead-letter-routing-key": f"node.{receiver.NODE_ID}",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }
    assert channel.qos == 1


def test_replay_all_resends_confirmed_events(session_factory):
    db = session_factory()
    db.add(Outbox(
        event_id="event-1", aggregate_id=RECORD_ID, version=1,
        event_type="ORDER_CREATED", payload={"item_name": "book", "price": 10},
        origin_node="node_a", updated_by_node="node_a", is_sent=True,
    ))
    db.commit()
    publisher = FakePublisher()
    publisher.fail = False

    sent = asyncio.run(publish_pending_events(
        db, publisher, peer_nodes=["node_b"], replay_all=True
    ))

    assert sent == ["event-1"]
    assert len(publisher.published) == 1
    assert publisher.published[0][1] == "node.node_b"


def test_receiver_processing_failure_nacks_with_requeue():
    message = FakeMessage(event("event-1", 1))

    asyncio.run(process_message(message, lambda: FailingSession()))

    assert message.acked == 1
    assert message.nacked == []
    assert message.rejected == []
    published, routing_key, mandatory = message.channel.default_exchange.published[0]
    assert routing_key == receiver.RETRY_QUEUE_NAME
    assert published.headers["app-retry-count"] == 1
    assert mandatory is True


def test_retry_limit_routes_event_to_dead_letter_queue(monkeypatch):
    monkeypatch.setattr(receiver, "MAX_RETRIES", 1)
    message = FakeMessage(event("event-1", 1), headers={"app-retry-count": 1})

    asyncio.run(process_message(message, lambda: FailingSession()))

    assert message.acked == 1
    published, routing_key, _ = message.channel.default_exchange.published[0]
    assert routing_key == receiver.DEAD_QUEUE_NAME
    assert "retry limit exceeded" in published.headers["app-dead-letter-reason"]


def test_retry_publish_failure_requeues_original_delivery():
    message = FakeMessage(event("event-1", 1))

    async def fail_publish(*args, **kwargs):
        raise RuntimeError("retry queue unavailable")

    message.channel.default_exchange.publish = fail_publish
    asyncio.run(process_message(message, lambda: FailingSession()))

    assert message.acked == 0
    assert message.nacked == [True]


def test_invalid_event_is_rejected_without_requeue(session_factory):
    message = FakeMessage({
        "event_id": "event-invalid",
        "aggregate_id": "not-a-uuid",
        "version": 1,
        "type": "ORDER_CREATED",
        "payload": {"item_name": "book", "price": 10},
    })

    asyncio.run(process_message(message, session_factory))

    assert message.acked == 1
    assert message.nacked == []
    published, routing_key, _ = message.channel.default_exchange.published[0]
    assert routing_key == receiver.DEAD_QUEUE_NAME
    assert "Invalid record UUID" in published.headers["app-dead-letter-reason"]


def test_missing_event_type_is_rejected_without_requeue(session_factory):
    invalid = event("event-invalid-type", 1)
    del invalid["type"]

    message = FakeMessage(invalid)
    asyncio.run(process_message(message, session_factory))

    assert message.acked == 1
    assert message.nacked == []
    assert message.channel.default_exchange.published[0][1] == receiver.DEAD_QUEUE_NAME


@pytest.mark.parametrize("price", [True, -1])
def test_invalid_price_is_dead_lettered(session_factory, price):
    invalid = event("event-invalid-price", 1, "ORDER_CREATED")
    invalid["payload"]["price"] = price
    message = FakeMessage(invalid)

    asyncio.run(process_message(message, session_factory))

    assert message.acked == 1
    assert message.channel.default_exchange.published[0][1] == receiver.DEAD_QUEUE_NAME


def test_duplicate_event_is_idempotent(session_factory):
    first = FakeMessage(event("event-1", 1, "ORDER_CREATED", "book"))
    second = FakeMessage(event("event-1", 1, "ORDER_CREATED", "changed"))

    asyncio.run(process_message(first, session_factory))
    asyncio.run(process_message(second, session_factory))

    db = session_factory()
    replica = db.query(Order).filter(Order.record_id == RECORD_ID).one()
    assert first.acked == 1
    assert second.acked == 1
    assert replica.item_name == "book"
    assert db.query(Inbox).count() == 1


def test_out_of_order_versions_keep_latest_replica(session_factory):
    for version in (3, 1, 2):
        asyncio.run(process_message(
            FakeMessage(event(f"event-{version}", version, item_name=f"item-{version}")),
            session_factory,
        ))

    replica = session_factory().query(Order).filter(Order.record_id == RECORD_ID).one()
    assert replica.version == 3
    assert replica.item_name == "item-3"


def test_duplicate_event_does_not_repeat_business_effect(session_factory):
    first = FakeMessage(event("event-1", 1, "ORDER_CREATED", "book"))
    duplicate = FakeMessage(event("event-1", 1, "ORDER_CREATED", "different"))

    asyncio.run(process_message(first, session_factory))
    asyncio.run(process_message(duplicate, session_factory))

    replica = session_factory().query(Order).filter(Order.record_id == RECORD_ID).one()
    assert replica.item_name == "book"
    assert replica.price == 1


def test_equal_versions_use_updated_by_node_tiebreak(session_factory):
    first_event = event("event-b", 2, item_name="writer-b")
    first_event["updated_by_node"] = "node_b"
    second_event = event("event-c", 2, item_name="writer-c")
    second_event["updated_by_node"] = "node_c"

    asyncio.run(process_message(FakeMessage(first_event), session_factory))
    asyncio.run(process_message(FakeMessage(second_event), session_factory))

    order = session_factory().query(Order).filter(Order.record_id == RECORD_ID).one()
    assert order.item_name == "writer-c"
    assert order.updated_by_node == "node_c"


def test_same_writer_and_version_use_event_id_tiebreak(session_factory):
    first_event = event("event-a", 2, item_name="writer-a")
    second_event = event("event-z", 2, item_name="writer-z")

    asyncio.run(process_message(FakeMessage(second_event), session_factory))
    asyncio.run(process_message(FakeMessage(first_event), session_factory))

    order = session_factory().query(Order).filter(Order.record_id == RECORD_ID).one()
    assert order.item_name == "writer-z"
    assert order.last_event_id == "event-z"


def test_delete_tombstone_rejects_older_update(session_factory):
    asyncio.run(process_message(
        FakeMessage(event("event-delete", 3, "ORDER_DELETED", "gone")), session_factory
    ))
    asyncio.run(process_message(
        FakeMessage(event("event-old", 2, "ORDER_UPDATED", "stale")), session_factory
    ))

    order = session_factory().query(Order).filter(Order.record_id == RECORD_ID).one()
    assert order.is_deleted is True
    assert order.version == 3
    assert order.item_name == "gone"


def test_api_crud_uses_one_uuid_and_soft_deletes(session_factory):
    db = session_factory()
    created = main.create_order(main.OrderCreate(item_name="book", price=10), db, None)
    local_id = created["order_id"]
    record_id = created["record_id"]

    main.update_order(record_id, main.OrderUpdate(item_name="notebook", price=12), db, None)
    main.delete_order(record_id, db, None)

    order = db.get(Order, local_id)
    events = db.query(Outbox).order_by(Outbox.id).all()
    manifest = main.get_manifest(db)
    assert order.record_id == record_id
    assert order.origin_node == main.NODE_ID
    assert order.is_deleted is True
    assert [event.event_type for event in events] == [
        "ORDER_CREATED", "ORDER_UPDATED", "ORDER_DELETED"
    ]
    assert {event.aggregate_id for event in events} == {record_id}
    assert manifest["count"] == 1
    assert manifest["records"][0]["record_id"] == record_id
    assert len(manifest["checksum"]) == 64


def test_sync_failure_keeps_outbox_pending(session_factory, monkeypatch):
    db = session_factory()
    main.create_order(main.OrderCreate(item_name="book", price=10), db, None)

    async def unavailable(*args, **kwargs):
        raise OSError("broker unavailable")

    monkeypatch.setattr(main.aio_pika, "connect_robust", unavailable)
    with pytest.raises(HTTPException) as error:
        asyncio.run(main.sync_events(False, db, None))

    assert error.value.status_code == 503
    assert db.query(Outbox).one().is_sent is False


def test_api_key_is_required_for_mutations(monkeypatch):
    monkeypatch.setenv("API_KEY", "test-secret")
    assert main.require_api_key("test-secret") is None
    with pytest.raises(HTTPException) as error:
        main.require_api_key("wrong-secret")
    assert error.value.status_code == 401


def test_order_input_validation_rejects_invalid_values():
    with pytest.raises(ValidationError):
        main.OrderCreate(item_name="", price=1)
    with pytest.raises(ValidationError):
        main.OrderUpdate(item_name="book", price=-1)
    with pytest.raises(ValidationError):
        main.OrderCreate(item_name="book", price=1.5)
    with pytest.raises(ValidationError):
        main.OrderCreate(item_name=123, price=1)
