import asyncio
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, Inbox, OrderReplica, Outbox
from receiver import process_message
from worker import publish_pending_events


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


class FakePublisher:
    def __init__(self):
        self.fail = True
        self.published = []

    async def publish(self, message, routing_key):
        if self.fail:
            self.fail = False
            raise RuntimeError("broker unavailable")
        self.published.append((message, routing_key))


class FakeMessage:
    def __init__(self, body):
        self.body = json.dumps(body).encode()
        self.acked = 0
        self.nacked = []
        self.rejected = []

    async def ack(self):
        self.acked += 1

    async def nack(self, requeue=False):
        self.nacked.append(requeue)

    async def reject(self, requeue=False):
        self.rejected.append(requeue)


def event(event_id, version, event_type="ORDER_UPDATED", item_name=None):
    return {
        "event_id": event_id,
        "aggregate_id": "1",
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
    assert len(publisher.published) == 1


def test_receiver_processing_failure_nacks_with_requeue(session_factory):
    message = FakeMessage({"event_id": "event-1", "type": "ORDER_UPDATED"})

    asyncio.run(process_message(message, session_factory))

    assert message.acked == 0
    assert message.nacked == [True]


def test_duplicate_event_is_idempotent(session_factory):
    first = FakeMessage(event("event-1", 1, "ORDER_CREATED", "book"))
    second = FakeMessage(event("event-1", 1, "ORDER_CREATED", "changed"))

    asyncio.run(process_message(first, session_factory))
    asyncio.run(process_message(second, session_factory))

    db = session_factory()
    replica = db.get(OrderReplica, 1)
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

    replica = session_factory().get(OrderReplica, 1)
    assert replica.version == 3
    assert replica.item_name == "item-3"


def test_duplicate_event_does_not_repeat_business_effect(session_factory):
    first = FakeMessage(event("event-1", 1, "ORDER_CREATED", "book"))
    duplicate = FakeMessage(event("event-1", 1, "ORDER_CREATED", "different"))

    asyncio.run(process_message(first, session_factory))
    asyncio.run(process_message(duplicate, session_factory))

    replica = session_factory().get(OrderReplica, 1)
    assert replica.item_name == "book"
    assert replica.price == 1
