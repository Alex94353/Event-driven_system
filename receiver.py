import asyncio
import json

import aio_pika
from sqlalchemy.orm import Session

from main import SessionLocal
from models import Inbox


async def connect_to_rabbitmq():
    for attempt in range(1, 31):
        try:
            return await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
        except Exception as exc:
            print(f"RabbitMQ not ready yet (attempt {attempt}/30), retrying in 2s...: {exc}")
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ did not become ready in time")


async def process_message(message: aio_pika.IncomingMessage):
    # Менеджер контекста process() автоматически отправляет ACK брокеру при успехе
    async with message.process():
        body = json.loads(message.body.decode())
        event_id = body.get("event_id")
        payload = body.get("payload")
        
        db: Session = SessionLocal()
        try:
            # 1. Проверяем Inbox: обрабатывали ли мы уже это событие?
            existing_event = db.query(Inbox).filter(Inbox.event_id == event_id).first()
            
            if existing_event:
                print(f" [!] Событие {event_id} уже было обработано. Пропускаем.")
                return

            # 2. Имитация полезной бизнес-логики (например, списание средств, отправка email)
            print(f" [v] Обрабатываем новый заказ: {payload['item_name']} за {payload['price']}")
            
            # 3. Сохраняем event_id в Inbox, чтобы защититься от дубликатов в будущем
            db.add(Inbox(event_id=event_id))
            db.commit()
            print(f" [+] Событие {event_id} зафиксировано в Inbox.\n")
            
        except Exception as e:
            print(f"Ошибка при обработке базы данных: {e}")
            db.rollback()
        finally:
            db.close()

async def main():
    # Подключаемся к RabbitMQ
    connection = await connect_to_rabbitmq()
    channel = await connection.channel()
    
    # Убеждаемся, что очередь существует
    queue = await channel.declare_queue("node_events", durable=True)
    
    print("Ресивер запущен: ожидаю сообщения из RabbitMQ...")
    # Начинаем слушать очередь
    await queue.consume(process_message)
    
    # Бесконечный цикл, чтобы скрипт не завершался
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())