import os
from urllib.parse import quote_plus


def get_database_url() -> str:
    """Return the configured node database URL."""
    configured_url = os.getenv("DATABASE_URL")
    if configured_url:
        return configured_url

    user = quote_plus(os.getenv("MYSQL_USER", "root"))
    password = quote_plus(os.getenv("MYSQL_PASSWORD", "root"))
    host = os.getenv("MYSQL_HOST", "mysql")
    port = os.getenv("MYSQL_PORT", "3306")
    database = os.getenv("MYSQL_DATABASE", "outbox_db")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


def get_rabbitmq_url() -> str:
    host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    port = os.getenv("RABBITMQ_PORT", "5672")
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASSWORD", "guest")
    vhost = os.getenv("RABBITMQ_VHOST", "/")
    return f"amqp://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}{vhost}"


RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "events")
