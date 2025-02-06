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
            pool_recycle=3600
        )
        
        # Add this line to create tables if they don't exist
        metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self, max_retries=3, retry_delay=5):
        """Context manager for database sessions with retry logic"""
        session = self.SessionLocal()
        retries = 0
        
        while retries < max_retries:
            try:
                yield session
                session.commit()
                return
            except Exception as e:
                session.rollback()
                retries += 1
                logger.error(f"Database error. Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})")
                logger.debug(str(e))
                time.sleep(retry_delay)
                if retries == max_retries:
                    raise
            finally:
                session.close()

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
            # Use INSERT IGNORE to skip duplicates
                stmt = insert(empresas).prefix_with('IGNORE')
                conn.execute(
                    stmt,
                    [
                        {
                            'codigo_infotel': item.get('COD_INFOTEL', ''),
                            'nif': item.get('NIF', ''),
                            'razon_social': item.get('RAZON_SOCIAL', ''),
                            'direccion': item.get('DOMICILIO', ''),
                            'codigo_postal': item.get('COD_POSTAL', ''),
                            'poblacion': item.get('NOM_POBLACION', ''),
                            'provincia': item.get('NOM_PROVINCIA', ''),
                            'website': item.get('URL', ''),
                            'url_valid': item.get('URL_VALID', False),
                            'confidence_score': item.get('confidence_score', 100)
                        }
                        for item in data
                    ]
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Bulk insert failed: {str(e)}")
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