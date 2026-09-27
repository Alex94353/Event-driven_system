from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional

import aio_pika
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field, StrictInt, StrictStr
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from config import RABBITMQ_EXCHANGE, get_database_url, get_peer_nodes, get_rabbitmq_url
from models import Inbox, Order, Outbox


DATABASE_URL = get_database_url()
NODE_ID = os.getenv("NODE_ID", "node_a")
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
    title=f"Node {NODE_ID} - Distributed System",
)


@app.get("/")
def health_check():
    return {"status": "ok", "node_id": NODE_ID}


@app.get("/health")
def health():
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return {"status": "healthy", "node_id": NODE_ID}


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class OrderCreate(BaseModel):
    item_name: StrictStr = Field(min_length=1, max_length=100)
    price: StrictInt = Field(ge=0)


class OrderUpdate(BaseModel):
    item_name: StrictStr = Field(min_length=1, max_length=100)
    price: StrictInt = Field(ge=0)


def require_api_key(x_api_key: Optional[str] = Header(default=None)):
    configured_key = os.getenv("API_KEY")
    if not configured_key:
        raise HTTPException(status_code=503, detail="API_KEY is not configured")
    if not x_api_key or not secrets.compare_digest(x_api_key, configured_key):
        raise HTTPException(status_code=401, detail="Invalid API key")


def add_outbox_event(db: Session, order: Order, event_type: str) -> Outbox:
    event_id = str(uuid.uuid4())
    order.last_event_id = event_id
    event = Outbox(
        event_id=event_id,
        aggregate_id=order.record_id,
        origin_node=order.origin_node,
        updated_by_node=order.updated_by_node,
        version=order.version,
        event_type=event_type,
        payload={
            "item_name": order.item_name,
            "price": order.price,
        },
    )
    db.add(event)
    return event


@app.post("/orders/")
def create_order(
    order: OrderCreate,
    db: Session = Depends(get_db),
    _api_key: None = Depends(require_api_key),
):
    new_order = Order(
        record_id=str(uuid.uuid4()),
        origin_node=NODE_ID,
        updated_by_node=NODE_ID,
        item_name=order.item_name,
        price=order.price,
        version=1,
    )
    db.add(new_order)
    db.flush()

    outbox_event = add_outbox_event(db, new_order, "ORDER_CREATED")
    db.commit()

    return {
        "message": "Transaction successful",
        "order_id": new_order.id,
        "record_id": new_order.record_id,
        "event_id": outbox_event.event_id,
    }


@app.get("/orders/")
def list_orders(
    include_deleted: bool = False,
    db: Session = Depends(get_db),
    _api_key: None = Depends(require_api_key),
):
    query = db.query(Order).order_by(Order.record_id)
    if not include_deleted:
        query = query.filter(Order.is_deleted.is_(False))
    return [
        {
            "order_id": order.id,
            "record_id": order.record_id,
            "origin_node": order.origin_node,
            "updated_by_node": order.updated_by_node,
            "item_name": order.item_name,
            "price": order.price,
            "version": order.version,
            "is_deleted": order.is_deleted,
        }
        for order in query.all()
    ]


@app.put("/orders/{record_id}")
def update_order(
    record_id: str,
    order: OrderUpdate,
    db: Session = Depends(get_db),
    _api_key: None = Depends(require_api_key),
):
    existing_order = (
        db.query(Order)
        .filter(Order.record_id == record_id)
        .with_for_update()
        .first()
    )
    if existing_order is None or existing_order.is_deleted:
        raise HTTPException(status_code=404, detail="Order not found")

    existing_order.item_name = order.item_name
    existing_order.price = order.price
    existing_order.version += 1
    existing_order.updated_by_node = NODE_ID
    outbox_event = add_outbox_event(db, existing_order, "ORDER_UPDATED")
    db.commit()
    return {
        "message": "Transaction successful",
        "order_id": existing_order.id,
        "record_id": existing_order.record_id,
        "event_id": outbox_event.event_id,
    }


@app.delete("/orders/{record_id}")
def delete_order(
    record_id: str,
    db: Session = Depends(get_db),
    _api_key: None = Depends(require_api_key),
):
    existing_order = (
        db.query(Order)
        .filter(Order.record_id == record_id)
        .with_for_update()
        .first()
    )
    if existing_order is None or existing_order.is_deleted:
        raise HTTPException(status_code=404, detail="Order not found")

    existing_order.version += 1
    existing_order.updated_by_node = NODE_ID
    existing_order.is_deleted = True
    outbox_event = add_outbox_event(db, existing_order, "ORDER_DELETED")
    db.commit()
    return {
        "message": "Transaction successful",
        "order_id": existing_order.id,
        "record_id": existing_order.record_id,
        "event_id": outbox_event.event_id,
    }


@app.get("/manifest")
def get_manifest(
    db: Session = Depends(get_db),
    _api_key: None = Depends(require_api_key),
):
    records = [
        {
            "record_id": order.record_id,
            "origin_node": order.origin_node,
            "updated_by_node": order.updated_by_node,
            "version": order.version,
            "is_deleted": order.is_deleted,
            "item_name": order.item_name,
            "price": order.price,
            "last_event_id": order.last_event_id,
        }
        for order in db.query(Order).order_by(Order.record_id).all()
    ]
    canonical = json.dumps(records, sort_keys=True, separators=(",", ":"))
    return {
        "node_id": NODE_ID,
        "count": len(records),
        "checksum": hashlib.sha256(canonical.encode()).hexdigest(),
        "records": records,
    }


@app.get("/status")
def get_status(
    db: Session = Depends(get_db),
    _api_key: None = Depends(require_api_key),
):
    oldest = (
        db.query(Outbox)
        .filter(Outbox.is_sent.is_(False))
        .order_by(Outbox.id)
        .first()
    )
    return {
        "node_id": NODE_ID,
        "orders": db.query(Order).count(),
        "pending_outbox": db.query(Outbox).filter(Outbox.is_sent.is_(False)).count(),
        "processed_events": db.query(Inbox).count(),
        "oldest_pending_outbox_id": oldest.id if oldest else None,
    }


@app.post("/sync")
async def sync_events(
    replay_all: bool = Query(default=False),
    db: Session = Depends(get_db),
    _api_key: None = Depends(require_api_key),
):
    from worker import publish_pending_events

    connection = None
    try:
        connection = await aio_pika.connect_robust(get_rabbitmq_url(), timeout=5)
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.declare_exchange(
            RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        sent_event_ids = await publish_pending_events(
            db,
            exchange,
            peer_nodes=get_peer_nodes(NODE_ID),
            replay_all=replay_all,
        )
        return {"status": "synchronized", "published_events": sent_event_ids}
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Broker unavailable or sync failed; unsent events remain in the outbox",
        ) from exc
    finally:
        if connection is not None:
            await connection.close()
