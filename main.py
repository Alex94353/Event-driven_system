import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from models import Base, Order, Outbox


# Строка подключения к MySQL в вашем Docker-контейнере
# Формат: mysql+драйвер://пользователь:пароль@хост:порт/имя_базы
DATABASE_URL = "mysql+pymysql://root:root@mysql:3306/outbox_db"

# Движок для общения с БД (echo=True будет выводить SQL-запросы в консоль для отладки)
engine = create_engine(DATABASE_URL, echo=True)

def wait_for_db(max_retries: int = 30, delay_seconds: int = 2):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except OperationalError as exc:
            last_error = exc
            print(f"MySQL not ready yet (attempt {attempt}/{max_retries}), retrying in {delay_seconds}s...")
            time.sleep(delay_seconds)
    raise RuntimeError("MySQL did not become ready in time") from last_error


# Жизненный цикл приложения: выполняется при старте сервера
@asynccontextmanager
async def lifespan(app: FastAPI):
    wait_for_db()
    # Берем все модели из Base и создаем под них таблицы в MySQL
    Base.metadata.create_all(bind=engine)
    print("Таблицы успешно созданы в MySQL!")
    yield

# Инициализируем FastAPI
app = FastAPI(lifespan=lifespan, title="Node A - Distributed System")

# Простейший эндпоинт для проверки работы
@app.get("/")
def health_check():
    return {"status": "ok", "message": "Node is running and connected to DB"}




# Фабрика сессий для подключения к БД
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Зависимость для получения сессии БД в каждом запросе
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Pydantic-схема для проверки того, что нам присылают по API
class OrderCreate(BaseModel):
    item_name: str
    price: int

# Тот самый эндпоинт с транзакцией Outbox
@app.post("/orders/")
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    # 1. Создаем саму бизнес-запись (Заказ)
    new_order = Order(item_name=order.item_name, price=order.price)
    db.add(new_order)
    db.flush()  # flush отправляет SQL в БД, чтобы получить ID заказа, но НЕ фиксирует транзакцию

    # 2. Создаем событие для отправки в RabbitMQ
    event_payload = {
        "order_id": new_order.id, 
        "item_name": new_order.item_name, 
        "price": new_order.price
    }
    
    outbox_event = Outbox(
        event_id=str(uuid.uuid4()),
        event_type="ORDER_CREATED",
        payload=event_payload
    )
    db.add(outbox_event)

    # 3. Фиксируем обе записи В ОДНОЙ АТОМАРНОЙ ТРАНЗАКЦИИ
    db.commit()
    
    return {
        "message": "Transaction successful", 
        "order_id": new_order.id,
        "event_id": outbox_event.event_id
    }