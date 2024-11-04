import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from utilities.config import DatabaseConfig, DATASET_TYPE, DATASET_DIR
from utilities.constants.database_enums import DatabaseType

HotelBase = declarative_base()
StoreBase = declarative_base()
HealthcareBase = declarative_base()
MusicFestivalBase = declarative_base()

engine = create_engine(f"sqlite:///{DatabaseConfig.DATABASE_URL}")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def set_database(database_name: str):
    """
    Update the active database and refresh the engine and sessionmaker.
    """
    if DATASET_TYPE in ["bird_dev", "bird_train"]:
        if database_name not in os.listdir(DATASET_DIR) or f"{database_name}.sqlite" not in os.listdir(f"{DATASET_DIR}/{database_name}"):
            raise ValueError(f"database_name {database_name} does not exist in {DATASET_DIR} or {database_name}.sqlite does not exist in {DATASET_DIR}/{database_name}")
    
    if DATASET_TYPE == "synthetic":
        if database_name not in [db_type.value for db_type in DatabaseType]:
            raise ValueError(f"database_name {database_name} is not a valid database type")

    DatabaseConfig.set_database(database_name)

    global engine, SessionLocal
    engine = create_engine(f"sqlite:///{DatabaseConfig.DATABASE_URL}")
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
