import asyncio
import json

import aio_pika
from sqlalchemy.orm import Session

from config import RABBITMQ_EXCHANGE, get_rabbitmq_url
from main import SessionLocal
from models import Outbox

async def connect_to_rabbitmq():
    for attempt in range(1, 31):
        try:
            return await aio_pika.connect_robust(get_rabbitmq_url())
        except Exception as exc:
            print(f"RabbitMQ not ready yet (attempt {attempt}/30), retrying in 2s...: {exc}")
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ did not become ready in time")


async def publish_pending_events(db: Session, exchange) -> list[str]:
    """Publish unsent outbox rows, marking each row sent only after confirmation."""
    unsent_events = db.query(Outbox).filter(Outbox.is_sent == False).all()
    sent_event_ids = []
    for event in unsent_events:
        message_body = json.dumps({
            "event_id": event.event_id,
            "aggregate_id": event.aggregate_id,
            "version": event.version,
            "type": event.event_type,
            "payload": event.payload,
        }).encode()
        try:
            await exchange.publish(
                aio_pika.Message(
                    body=message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="orders",
            )
        except Exception:
            db.rollback()
            raise
        event.is_sent = True
        db.commit()
        sent_event_ids.append(event.event_id)
    return sent_event_ids


async def main():
    connection = await connect_to_rabbitmq()
    channel = await connection.channel(publisher_confirms=True)

    exchange = await channel.declare_exchange(
        RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )

    print("Worker started: checking Outbox table...")

    while True:
        db: Session = SessionLocal()
        try:
            sent_event_ids = await publish_pending_events(db, exchange)
            for event_id in sent_event_ids:
                print(f" [+] Event {event_id} successfully sent to broker!")

        except Exception as exc:
            print(f"Worker error: {exc}")
            db.rollback()
        finally:
            db.close()

        await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
