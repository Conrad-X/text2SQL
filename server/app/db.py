import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from utilities.config import DatabaseConfig, DATASET_TYPE, DATASET_DIR
from utilities.constants.database_enums import DatabaseType, DatasetType
from utilities.constants.response_messages import ERROR_DATABASE_NOT_FOUND

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
    if DATASET_TYPE in [DatasetType.BIRD_DEV, DatasetType.BIRD_TRAIN]:
        if database_name not in os.listdir(DATASET_DIR) or f"{database_name}.sqlite" not in os.listdir(f"{DATASET_DIR}/{database_name}"):
            raise ValueError(ERROR_DATABASE_NOT_FOUND.format(database_name=database_name))
    
    if DATASET_TYPE == DatasetType.SYNTHETIC:
        if database_name not in [db_type.value for db_type in DatabaseType]:
            raise ValueError(ERROR_DATABASE_NOT_FOUND.format(database_name=database_name))

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