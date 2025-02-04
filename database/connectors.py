# database/connectors.py
import pyodbc
from sqlalchemy import create_engine
from contextlib import contextmanager
from config import Config
import logging
import time

logger = logging.getLogger(__name__)

class SQLServerConnector:
    def __init__(self):
        self.connection_string = Config.SQL_SERVER_CONN_STR
        self.engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={self.connection_string}",
            fast_executemany=True
        )

    @contextmanager
    def get_connection(self, max_retries=3, retry_delay=5):
        """Context manager for transactional connections with retry logic"""
        retries = 0
        while retries < max_retries:
            try:
                conn = self.engine.connect()
                yield conn
                return
            except pyodbc.OperationalError as e:
                retries += 1
                logger.error(f"SQL Server connection failed. Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})")
                logger.debug(str(e))
                time.sleep(retry_delay)
        logger.error("Maximum number of retries exceeded. Unable to connect to SQL Server.")
        raise Exception("Failed to connect to SQL Server after multiple retries.")

    def test_connection(self):
        """Prueba básica de conectividad"""
        try:
            with self.get_connection() as conn:
                result = conn.execute("SELECT 1 AS test;")
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Error en test de conexión: {e}")
            return False

    def bulk_insert_companies(self, data):
        """Inserción masiva de datos usando executemany"""
        insert_query = """
        INSERT INTO empresas (
            nif, razon_social, provincia, website, 
            telefonos, redes_sociales, ecommerce, confidence_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            with self.get_connection() as conn:
                conn.execute(
                    insert_query,
                    [(
                        item['nif'], item['razon_social'], item['provincia'],
                        item['website'], item['telefonos'], item['redes_sociales'],
                        item['ecommerce'], item['confidence_score']
                    ) for item in data]
                )
                conn.commit()
                return True
        except pyodbc.IntegrityError as e:
            logger.warning(f"Duplicado detectado: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error en inserción masiva: {str(e)}")
            raise

# Ejemplo de uso:
if __name__ == "__main__":
    connector = SQLServerConnector()
    if connector.test_connection():
        print("✅ Conexión exitosa a SQL Server")
    else:
        print("❌ Error de conexión")