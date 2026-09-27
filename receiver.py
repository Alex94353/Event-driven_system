from __future__ import annotations

import asyncio
import json
import os
from typing import Optional
from uuid import UUID

import aio_pika
from sqlalchemy.orm import Session

from config import RABBITMQ_EXCHANGE, get_rabbitmq_url
from main import SessionLocal
from models import Inbox, Order

NODE_ID = os.getenv("NODE_ID", "node_a")
QUEUE_NAME = f"{NODE_ID}.events"
RETRY_QUEUE_NAME = f"{NODE_ID}.events.retry"
DEAD_QUEUE_NAME = f"{NODE_ID}.events.dead"
MAX_RETRIES = int(os.getenv("MESSAGE_MAX_RETRIES", "5"))
RETRY_DELAY_MS = int(os.getenv("MESSAGE_RETRY_DELAY_MS", "5000"))

if MAX_RETRIES < 0:
    raise ValueError("MESSAGE_MAX_RETRIES must be non-negative")
if RETRY_DELAY_MS < 1:
    raise ValueError("MESSAGE_RETRY_DELAY_MS must be positive")


class InvalidEvent(ValueError):
    pass


async def publish_to_queue(
    message: aio_pika.IncomingMessage,
    queue_name: str,
    headers: dict,
    publisher_channel=None,
):
    retry_message = aio_pika.Message(
        body=message.body,
        headers=headers,
        content_type=message.content_type,
        correlation_id=message.correlation_id,
        message_id=message.message_id,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    channel = publisher_channel or message.channel
    default_exchange = await channel.get_exchange("", ensure=False)
    await default_exchange.publish(
        retry_message,
        routing_key=queue_name,
        mandatory=True,
    )


async def dead_letter(
    message: aio_pika.IncomingMessage,
    reason: str,
    publisher_channel=None,
) -> None:
    headers = dict(message.headers or {})
    headers["app-dead-letter-reason"] = reason[:500]
    await publish_to_queue(message, DEAD_QUEUE_NAME, headers, publisher_channel)
    await message.ack()


async def reject_or_requeue(
    message: aio_pika.IncomingMessage,
    reason: str,
    publisher_channel=None,
) -> None:
    try:
        await dead_letter(message, reason, publisher_channel)
    except Exception as publish_exc:
        print(f"Could not dead-letter message; requeueing: {publish_exc}")
        await message.nack(requeue=True)


async def retry_or_dead_letter(
    message: aio_pika.IncomingMessage,
    error: Exception,
    publisher_channel=None,
) -> None:
    headers = dict(message.headers or {})
    try:
        retry_count = int(headers.get("app-retry-count", 0))
    except (TypeError, ValueError) as exc:
        raise InvalidEvent("Invalid app-retry-count header") from exc
    if retry_count < 0:
        raise InvalidEvent("Invalid app-retry-count header")

    if retry_count >= MAX_RETRIES:
        await dead_letter(message, f"retry limit exceeded: {error}", publisher_channel)
        return

    headers["app-retry-count"] = retry_count + 1
    await publish_to_queue(message, RETRY_QUEUE_NAME, headers, publisher_channel)
    await message.ack()


async def connect_to_rabbitmq():
    for attempt in range(1, 31):
        try:
            return await aio_pika.connect_robust(get_rabbitmq_url(), timeout=5, heartbeat=5)
        except Exception as exc:
            print(f"RabbitMQ not ready yet (attempt {attempt}/30), retrying in 2s...: {exc}")
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ did not become ready in time")


async def declare_receiver_topology(channel):
    exchange = await channel.declare_exchange(
        RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await channel.declare_queue(
        RETRY_QUEUE_NAME,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-message-ttl": RETRY_DELAY_MS,
            "x-dead-letter-exchange": RABBITMQ_EXCHANGE,
            "x-dead-letter-routing-key": f"node.{NODE_ID}",
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )
    await channel.declare_queue(DEAD_QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key=f"node.{NODE_ID}")
    await channel.set_qos(prefetch_count=1)
    return queue


async def process_message(
    message: aio_pika.IncomingMessage,
    session_factory=SessionLocal,
    publisher_channel=None,
):
    db: Optional[Session] = None
    try:
        db = session_factory()
        try:
            body = json.loads(message.body.decode())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidEvent("Malformed JSON event") from exc
        if not isinstance(body, dict):
            raise InvalidEvent("Event body must be an object")
        event_id = body.get("event_id")
        try:
            record_id = str(UUID(str(body["aggregate_id"])))
            version = body["version"]
            event_type = body["type"]
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidEvent("Invalid record UUID, version, or event type") from exc
        payload = body.get("payload")
        origin_node = body.get("origin_node", "legacy")
        updated_by_node = body.get("updated_by_node", origin_node)
        if (
            not isinstance(event_id, str)
            or not event_id
            or len(event_id) > 36
            or type(version) is not int
            or version < 1
            or not isinstance(payload, dict)
            or not isinstance(payload.get("item_name"), str)
            or len(payload["item_name"]) > 100
            or type(payload.get("price")) is not int
            or payload["price"] < 0
            or not isinstance(event_type, str)
            or event_type not in {"ORDER_CREATED", "ORDER_UPDATED", "ORDER_DELETED"}
            or not isinstance(origin_node, str)
            or not origin_node
            or len(origin_node) > 100
            or not isinstance(updated_by_node, str)
            or not updated_by_node
            or len(updated_by_node) > 100
        ):
            raise InvalidEvent("Malformed order event")

        with db.begin():
            existing_event = db.query(Inbox).filter(Inbox.event_id == event_id).first()
            if existing_event:
                print(f" [!] Event {event_id} already processed. Skipping.")
            else:
                order = db.query(Order).filter(Order.record_id == record_id).first()
                if order is None:
                    order = Order(
                        record_id=record_id,
                        origin_node=origin_node,
                        updated_by_node=updated_by_node,
                        item_name=payload["item_name"],
                        price=payload["price"],
                        version=version,
                        is_deleted=event_type == "ORDER_DELETED",
                        last_event_id=event_id,
                    )
                    db.add(order)
                    print(f" [v] Applied {event_type} v{version} for record {record_id}")
                else:
                    if order.origin_node != origin_node:
                        raise InvalidEvent("Record origin does not match existing record")
                    current_order_key = (
                        order.version,
                        order.updated_by_node,
                        order.last_event_id or "",
                    )
                    incoming_order_key = (version, updated_by_node, event_id)
                    if incoming_order_key > current_order_key:
                        order.item_name = payload["item_name"]
                        order.price = payload["price"]
                        order.version = version
                        order.updated_by_node = updated_by_node
                        order.is_deleted = event_type == "ORDER_DELETED"
                        order.last_event_id = event_id
                        print(f" [v] Applied {event_type} v{version} for record {record_id}")
                    else:
                        print(f" [!] Ignoring stale event {event_id} v{version} for record {record_id}")

                db.add(Inbox(event_id=event_id))

        await message.ack()
        print(f" [+] Event {event_id} recorded in Inbox.\n")

    except InvalidEvent as exc:
        if db is not None:
            db.rollback()
        print(f"Rejecting invalid event: {exc}")
        await reject_or_requeue(message, str(exc), publisher_channel)

    except Exception as exc:
        if db is not None:
            db.rollback()
        print(f"Error processing message; scheduling retry: {exc}")
        try:
            await retry_or_dead_letter(message, exc, publisher_channel)
        except InvalidEvent as invalid_header:
            await reject_or_requeue(message, str(invalid_header), publisher_channel)
        except Exception as publish_exc:
            print(f"Could not schedule retry; requeueing: {publish_exc}")
            await message.nack(requeue=True)
    finally:
        if db is not None:
            db.close()


async def main():
    connection = await connect_to_rabbitmq()
    channel = await connection.channel(publisher_confirms=True)
    queue = await declare_receiver_topology(channel)
    print("Receiver started: waiting for messages from RabbitMQ...")

    async def on_message(message: aio_pika.IncomingMessage) -> None:
        await process_message(message, publisher_channel=channel)

    await queue.consume(on_message)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
