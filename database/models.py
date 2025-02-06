# database/models.py
from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, DateTime, Text, ForeignKey
from sqlalchemy.sql import func

metadata = MetaData()

empresas = Table(
    'empresas', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('codigo_infotel', String(20), unique=True, nullable=False),
    Column('nif', String(15), nullable=False),
    Column('razon_social', String(255), nullable=False),
    Column('direccion', Text),
    Column('codigo_postal', String(5)),
    Column('poblacion', String(100)),
    Column('provincia', String(50)),
    Column('website', String(500)),
    Column('url_valid', Boolean, default=False),
    Column('telefonos', Text),
    Column('redes_sociales', Text),
    Column('ecommerce', Boolean, default=False),
    Column('fecha_actualizacion', DateTime, server_default=func.now(), onupdate=func.now()),
    Column('confidence_score', Integer)
)

scraping_logs = Table(
    'scraping_logs', metadata,
    Column('log_id', Integer, primary_key=True, autoincrement=True),
    Column('empresa_id', Integer, ForeignKey('empresas.id', ondelete='CASCADE'), nullable=False),
    Column('status', String(50)),
    Column('intentos', Integer, default=0),
    Column('detalle_error', Text),
    Column('timestamp', DateTime, server_default=func.now())
)