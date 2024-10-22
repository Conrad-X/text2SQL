import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from utilities.constants.database_enums import DatabaseType
from utilities.config import DatabaseConfig

HotelBase = declarative_base()
StoreBase = declarative_base()
HealthcareBase = declarative_base()
MusicFestivalBase = declarative_base()

engine = create_engine(f"sqlite:///{DatabaseConfig.DATABASE_URL}")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def set_database(database_type: DatabaseType):
    """
    Update the active database and refresh the engine and sessionmaker.
    """
    DatabaseConfig.set_database(database_type)

    global engine, SessionLocal
    engine = create_engine(f"sqlite:///{DatabaseConfig.DATABASE_URL}")
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
