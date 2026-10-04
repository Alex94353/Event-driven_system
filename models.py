from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import String, Integer, JSON, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

# Business records and replication metadata.
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

# Store events atomically with order changes.
class Outbox(Base):
    __tablename__ = "outbox"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), unique=True)
    aggregate_id: Mapped[str] = mapped_column(String(36), nullable=False)
    origin_node: Mapped[str] = mapped_column(String(100), default="legacy", nullable=False)
    updated_by_node: Mapped[str] = mapped_column(String(100), default="legacy", nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(50))            # Order lifecycle event.
    payload: Mapped[dict] = mapped_column(JSON)                    # Serialized order data.
    is_sent: Mapped[bool] = mapped_column(Boolean, default=False)  # Set after broker confirmation.

# Track processed event IDs to prevent duplicate effects.
class Inbox(Base):
    __tablename__ = "inbox"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), unique=True)