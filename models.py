from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import String, Integer, JSON, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Базовый класс для всех моделей
class Base(DeclarativeBase):
    pass

# Таблица для бизнес-логики (например, заказы)
class Order(Base):
    __tablename__ = "orders"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    record_id: Mapped[str] = mapped_column(String(36), unique=True, default=lambda: str(uuid.uuid4()))
    origin_node: Mapped[str] = mapped_column(String(100), default="legacy", nullable=False)
    updated_by_node: Mapped[str] = mapped_column(String(100), default="legacy", nullable=False)
    item_name: Mapped[str] = mapped_column(String(100))
    price: Mapped[int] = mapped_column(Integer)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_event_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)

# Таблица Outbox: сюда мы в одной транзакции пишем событие, чтобы потом отправить его в RabbitMQ
class Outbox(Base):
    __tablename__ = "outbox"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), unique=True) # UUID события
    aggregate_id: Mapped[str] = mapped_column(String(36), nullable=False)
    origin_node: Mapped[str] = mapped_column(String(100), default="legacy", nullable=False)
    updated_by_node: Mapped[str] = mapped_column(String(100), default="legacy", nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(50))            # Например, 'ORDER_CREATED'
    payload: Mapped[dict] = mapped_column(JSON)                    # Сами данные (JSON)
    is_sent: Mapped[bool] = mapped_column(Boolean, default=False)  # Отправлено ли в брокер?

# Таблица Inbox: сюда принимающий узел пишет ID событий, которые уже обработал (для идемпотентности)
class Inbox(Base):
    __tablename__ = "inbox"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), unique=True) # UUID обработанного события