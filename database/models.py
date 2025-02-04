# database/models.py
from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, DateTime
from sqlalchemy.dialects.mssql import TINYINT

metadata = MetaData()

empresas = Table(
    'empresas', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('nif', String(15), unique=True, nullable=False),
    Column('razon_social', String(255), nullable=False),
    Column('provincia', String(50)),
    Column('website', String(500)),
    Column('telefonos', String(500)),
    Column('redes_sociales', String(1000)),
    Column('ecommerce', Boolean),
    Column('fecha_actualizacion', DateTime, server_default='GETDATE()'),
    Column('confidence_score', TINYINT)
)

scraping_logs = Table(
    'scraping_logs', metadata,
    Column('log_id', Integer, primary_key=True, autoincrement=True),
    Column('empresa_id', Integer, nullable=False),
    Column('status', String(50)),
    Column('intentos', TINYINT),
    Column('detalle_error', String(500)),
    Column('timestamp', DateTime, server_default='GETDATE()')
)