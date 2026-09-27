import asyncio
import json
import os

import aio_pika
from sqlalchemy.orm import Session

from config import RABBITMQ_EXCHANGE, get_rabbitmq_url
from main import SessionLocal
from models import Inbox, OrderReplica

NODE_ID = os.getenv("NODE_ID", "node_a")
QUEUE_NAME = f"{NODE_ID}.events"


async def connect_to_rabbitmq():
    for attempt in range(1, 31):
        try:
            return await aio_pika.connect_robust(get_rabbitmq_url())
        except Exception as exc:
            print(f"RabbitMQ not ready yet (attempt {attempt}/30), retrying in 2s...: {exc}")
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ did not become ready in time")


async def process_message(message: aio_pika.IncomingMessage, session_factory=SessionLocal):
    db: Session = session_factory()
    try:
        body = json.loads(message.body.decode())
        event_id = body.get("event_id")
        order_id = int(body["aggregate_id"])
        version = int(body["version"])
        event_type = body["type"]
        payload = body.get("payload")
        if not event_id or payload is None or event_type not in {"ORDER_CREATED", "ORDER_UPDATED", "ORDER_DELETED"}:
            await message.reject(requeue=False)
            return

        with db.begin():
            existing_event = db.query(Inbox).filter(Inbox.event_id == event_id).first()
            if existing_event:
                print(f" [!] Event {event_id} already processed. Skipping.")
            else:
                replica = db.get(OrderReplica, order_id)
                if replica is None or version > replica.version:
                    if replica is None:
                        replica = OrderReplica(order_id=order_id, version=version)
                        db.add(replica)
                    replica.item_name = payload.get("item_name")
                    replica.price = payload.get("price")
                    replica.version = version
                    replica.is_deleted = event_type == "ORDER_DELETED"
                    print(f" [v] Applied {event_type} v{version} for order {order_id}")
                else:
                    print(f" [!] Ignoring stale event {event_id} v{version}; replica is v{replica.version}")

                db.add(Inbox(event_id=event_id))

        await message.ack()
        print(f" [+] Event {event_id} recorded in Inbox.\n")

    except Exception as exc:
        db.rollback()
        print(f"Error processing message; requeueing: {exc}")
        await message.nack(requeue=True)
    finally:
        db.close()


async def main():
    connection = await connect_to_rabbitmq()
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key="orders")
    await channel.set_qos(prefetch_count=1)

    print("Receiver started: waiting for messages from RabbitMQ...")
    await queue.consume(process_message)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
