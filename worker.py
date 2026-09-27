from __future__ import annotations

import asyncio
import json
import os
from typing import Optional

import aio_pika
from sqlalchemy.orm import Session

from config import RABBITMQ_EXCHANGE, get_peer_nodes, get_rabbitmq_url
from main import SessionLocal
from models import Outbox

async def connect_to_rabbitmq():
    for attempt in range(1, 31):
        try:
            return await aio_pika.connect_robust(get_rabbitmq_url(), timeout=5, heartbeat=5)
        except Exception as exc:
            print(f"RabbitMQ not ready yet (attempt {attempt}/30), retrying in 2s...: {exc}")
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ did not become ready in time")


async def publish_pending_events(
    db: Session,
    exchange,
    peer_nodes: Optional[list[str]] = None,
    replay_all: bool = False,
) -> list[str]:
    """Publish events to every peer and mark them sent only after all confirms."""
    local_node = os.getenv("NODE_ID", "node_a")
    peer_nodes = get_peer_nodes(local_node) if peer_nodes is None else peer_nodes
    query = db.query(Outbox).order_by(Outbox.id)
    if not replay_all:
        query = query.filter(Outbox.is_sent.is_(False))
    unsent_events = query.all()
    sent_event_ids = []
    for event in unsent_events:
        message_body = json.dumps({
            "event_id": event.event_id,
            "aggregate_id": event.aggregate_id,
            "origin_node": event.origin_node,
            "updated_by_node": event.updated_by_node,
            "version": event.version,
            "type": event.event_type,
            "payload": event.payload,
        }).encode()
        try:
            for peer_node in peer_nodes:
                await exchange.publish(
                    aio_pika.Message(
                        body=message_body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=f"node.{peer_node}",
                    mandatory=True,
                )
        except Exception:
            db.rollback()
            raise
        event.is_sent = True
        db.commit()
        sent_event_ids.append(event.event_id)
    return sent_event_ids


async def main():
    while True:
        connection = None
        try:
            connection = await connect_to_rabbitmq()
            channel = await connection.channel(publisher_confirms=True)
            exchange = await channel.declare_exchange(
                RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
            )
            print("Worker connected; checking Outbox table...", flush=True)
            db = SessionLocal()
            try:
                await asyncio.wait_for(
                    publish_pending_events(db, exchange, replay_all=True),
                    timeout=10,
                )
            finally:
                db.close()
            while True:
                db: Session = SessionLocal()
                try:
                    sent_event_ids = await asyncio.wait_for(
                        publish_pending_events(db, exchange), timeout=10
                    )
                    for event_id in sent_event_ids:
                        print(f" [+] Event {event_id} successfully sent to broker!", flush=True)
                except Exception as exc:
                    db.rollback()
                    raise RuntimeError(f"Publish failed; reconnecting: {exc}") from exc
                finally:
                    db.close()
                await asyncio.sleep(3)
        except Exception as exc:
            print(f"Worker connection error: {exc}", flush=True)
        finally:
            if connection is not None and not connection.is_closed:
                await connection.close()
        await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
