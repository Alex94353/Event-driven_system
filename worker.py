import asyncio
import json

import aio_pika
from sqlalchemy.orm import Session

from main import SessionLocal
from models import Outbox


async def connect_to_rabbitmq():
    for attempt in range(1, 31):
        try:
            return await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
        except Exception as exc:
            print(f"RabbitMQ not ready yet (attempt {attempt}/30), retrying in 2s...: {exc}")
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ did not become ready in time")


async def main():
    # 1. Подключаемся к нашему контейнеру RabbitMQ
    connection = await connect_to_rabbitmq()
    channel = await connection.channel()
    
    # 2. Объявляем очередь (если ее нет в брокере, она создастся автоматически)
    queue_name = "node_events"
    await channel.declare_queue(queue_name, durable=True)
    
    print("Воркер запущен: проверяю таблицу Outbox...")
    
    while True:
        # Открываем сессию БД
        db: Session = SessionLocal()
        try:
            # Ищем все записи, которые еще не были отправлены
            unsent_events = db.query(Outbox).filter(Outbox.is_sent == False).all()
            
            for event in unsent_events:
                # Пакуем данные в JSON
                message_body = json.dumps({
                    "event_id": event.event_id,
                    "type": event.event_type,
                    "payload": event.payload
                }).encode()
                
                # Отправляем сообщение в RabbitMQ с гарантией сохранения на диск (PERSISTENT)
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message_body, 
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key=queue_name
                )
                
                # Обновляем статус в базе данных, чтобы не отправить повторно
                event.is_sent = True
                db.commit()
                print(f" [+] Событие {event.event_id} успешно отправлено в брокер!")
                
        except Exception as e:
            print(f"Ошибка в воркере: {e}")
        finally:
            db.close()
            
        # Ждем 3 секунды перед следующей проверкой
        await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())