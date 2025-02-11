from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Sociedad(Base):
    __tablename__ = 'empresas'

    # Original columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    COD_INFOTEL = Column(Integer, nullable=False)
    NIF = Column(String(11), nullable=True)
    RAZON_SOCIAL = Column(String(255), nullable=True)
    DOMICILIO = Column(String(255), nullable=True)
    COD_POSTAL = Column(String(5), nullable=True)
    NOM_POBLACION = Column(String(100), nullable=True)
    NOM_PROVINCIA = Column(String(100), nullable=True)
    URL = Column(String(255), nullable=True)
    
    # New columns
    URL_EXISTS = Column(Boolean, default=False, nullable=False)
    URL_LIMPIA = Column(String(255), nullable=True)
    URL_STATUS = Column(Integer, nullable=True)
    URL_STATUS_MENSAJE = Column(String(255), nullable=True)
    TELEFONO_1 = Column(String(16), nullable=True)
    TELEFONO_2 = Column(String(16), nullable=True)
    TELEFONO_3 = Column(String(16), nullable=True)
    FACEBOOK = Column(String(255), nullable=True)
    TWITTER = Column(String(255), nullable=True)
    LINKEDIN = Column(String(255), nullable=True)
    INSTAGRAM = Column(String(255), nullable=True)
    YOUTUBE = Column(String(255), nullable=True)
    E_COMMERCE = Column(Boolean, default=False, nullable=False)

def create_table(engine):
    """
    Drop and recreate the table
    
    Args:
        engine: SQLAlchemy engine
    """
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("Table 'empresas' recreated with all columns")