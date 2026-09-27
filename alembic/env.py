from logging.config import fileConfig
import time

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.exc import OperationalError

from config import get_database_url
from models import Base


config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)
config.set_main_option("sqlalchemy.url", get_database_url().replace("%", "%%"))
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    last_error = None
    for attempt in range(1, 31):
        try:
            with connectable.connect() as connection:
                context.configure(connection=connection, target_metadata=target_metadata)
                with context.begin_transaction():
                    context.run_migrations()
            return
        except OperationalError as exc:
            last_error = exc
            if attempt == 30:
                raise
            print(f"Database not ready (attempt {attempt}/30), retrying in 2s...", flush=True)
            time.sleep(2)
    raise RuntimeError("Database did not become ready in time") from last_error


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
