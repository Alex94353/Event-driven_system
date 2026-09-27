import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from config import get_database_url
from models import Order, Outbox


DATABASE_URL = get_database_url()
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    wait_for_db()
    yield


app = FastAPI(
    lifespan=lifespan,
    title=f"Node {os.getenv('NODE_ID', 'node_a')} - Distributed System",
)


@app.get("/")
def health_check():
    return {"status": "ok", "message": "Node is running and connected to DB"}


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class OrderCreate(BaseModel):
    item_name: str
    price: int


class OrderUpdate(BaseModel):
    item_name: str
    price: int


def add_outbox_event(db: Session, order: Order, event_type: str) -> Outbox:
    event = Outbox(
        event_id=str(uuid.uuid4()),
        aggregate_id=str(order.id),
        version=order.version,
        event_type=event_type,
        payload={
            "order_id": order.id,
            "item_name": order.item_name,
            "price": order.price,
        },
    )
    db.add(event)
    return event


@app.post("/orders/")
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    new_order = Order(item_name=order.item_name, price=order.price)
    db.add(new_order)
    db.flush()

    outbox_event = add_outbox_event(db, new_order, "ORDER_CREATED")
    db.commit()

    return {
        "message": "Transaction successful",
        "order_id": new_order.id,
        "event_id": outbox_event.event_id,
    }


@app.put("/orders/{order_id}")
def update_order(order_id: int, order: OrderUpdate, db: Session = Depends(get_db)):
    existing_order = db.get(Order, order_id)
    if existing_order is None:
        return {"error": "Order not found"}

    existing_order.item_name = order.item_name
    existing_order.price = order.price
    existing_order.version += 1
    outbox_event = add_outbox_event(db, existing_order, "ORDER_UPDATED")
    db.commit()
    return {"message": "Transaction successful", "order_id": order_id, "event_id": outbox_event.event_id}


@app.delete("/orders/{order_id}")
def delete_order(order_id: int, db: Session = Depends(get_db)):
    existing_order = db.get(Order, order_id)
    if existing_order is None:
        return {"error": "Order not found"}

    existing_order.version += 1
    outbox_event = add_outbox_event(db, existing_order, "ORDER_DELETED")
    db.delete(existing_order)
    db.commit()
    return {"message": "Transaction successful", "order_id": order_id, "event_id": outbox_event.event_id}
