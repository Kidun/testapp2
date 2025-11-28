"""
Database models and connection management using SQLAlchemy ORM
"""
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from datetime import datetime
import os

Base = declarative_base()

# Database connection string (SQLite file-based database)
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:////opt/airflow/data/pipeline_data.db')

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class DataRecord(Base):
    """
    Main table for storing collected data from 3 sources
    At least 10 data columns required
    """
    __tablename__ = 'data_records'

    id = Column(Integer, primary_key=True, index=True)
    parse_time = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Source 1: Weather data (3 columns)
    temperature = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    wind_speed = Column(Float, nullable=True)

    # Source 2: Currency rates (4 columns)
    usd_rate = Column(Float, nullable=True)
    eur_rate = Column(Float, nullable=True)
    gbp_rate = Column(Float, nullable=True)
    cny_rate = Column(Float, nullable=True)

    # Source 3: Stock/Crypto prices (3 columns)
    btc_price = Column(Float, nullable=True)
    eth_price = Column(Float, nullable=True)
    stock_index = Column(Float, nullable=True)

    # Metadata columns (not counted in 10 required columns)
    failed_requests = Column(Integer, default=0, nullable=False)
    source1_response_time = Column(Float, nullable=True)
    source2_response_time = Column(Float, nullable=True)
    source3_response_time = Column(Float, nullable=True)
    source1_success = Column(Boolean, default=True)
    source2_success = Column(Boolean, default=True)
    source3_success = Column(Boolean, default=True)


@contextmanager
def get_db():
    """
    Context manager for database sessions
    Ensures proper connection cleanup
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)


if __name__ == '__main__':
    init_db()
    print("Database initialized successfully")
