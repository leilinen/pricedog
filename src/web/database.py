import logging
import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@postgres:5432/pricedog",
)

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    Base.metadata.create_all(bind=engine)
    _create_indexes()
    logger.info("PostgreSQL database initialized")


def _create_indexes():
    indexes = [
        """
CREATE INDEX IF NOT EXISTS ix_stocks_market_symbol
ON stocks(market, symbol)
""",
        """
CREATE INDEX IF NOT EXISTS ix_positions_account_stock
ON positions(account_id, stock_id)
""",
        """
CREATE INDEX IF NOT EXISTS ix_price_alert_rules_enabled_updated
ON price_alert_rules(enabled, updated_at)
""",
    ]
    with engine.begin() as conn:
        for sql in indexes:
            conn.execute(text(sql))
