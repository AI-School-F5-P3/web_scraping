# database/connectors.py
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from config import Config
import logging
import time
from typing import List, Dict, Any
from .models import metadata, empresas
import traceback

logger = logging.getLogger(__name__)

class MySQLConnector:
    def __init__(self):
        self.engine = create_engine(
            Config.SQLALCHEMY_DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True  # Add connection testing
        )
        
        # Add this line to create tables if they don't exist
        metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self, max_retries=3, retry_delay=5):
        session = self.SessionLocal()
        retries = 0
        
        try:
            while retries < max_retries:
                try:
                    yield session
                    session.commit()
                    break
                except Exception as e:
                    session.rollback()
                    if retries == max_retries - 1:
                        raise
                    retries += 1
                    logger.warning(f"Retry {retries}/{max_retries} - Error: {str(e)}")
                    time.sleep(retry_delay ** retries)
        finally:
            session.close()
            if session.is_active:
                session.rollback()

    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            with self.get_session() as session:
                session.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def bulk_insert_companies(self, data: List[Dict[str, Any]]) -> bool:
        try:
            with self.engine.connect() as conn:
                # Add data validation
                for item in data:
                    if not all(k in item for k in ['NIF', 'RAZON_SOCIAL']):
                        raise ValueError("Missing required fields in data")
                
                stmt = insert(empresas).prefix_with('IGNORE')
                result = conn.execute(stmt, data)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Bulk insert failed: {str(e)}\n{traceback.format_exc()}")
            return False
        
def delete_companies(self, criteria: Dict[str, Any]) -> int:
    """Erase companies matching criteria"""
    try:
        with self.engine.connect() as conn:
            stmt = empresas.delete()
            for key, value in criteria.items():
                stmt = stmt.where(getattr(empresas.c, key) == value)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount
    except Exception as e:
        logger.error(f"Delete failed: {str(e)}")
        return 0