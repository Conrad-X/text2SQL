import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utilities.config import ACTIVE_DATABASE
from utilities.constants.database_enums import DATABASE_PATHS

DATABASE_URL = DATABASE_PATHS.get(ACTIVE_DATABASE)

engine = create_engine(f"sqlite:///{DATABASE_URL}")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

HotelBase = declarative_base()
StoreBase = declarative_base()
HealthcareBase = declarative_base()
MusicFestivalBase = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
