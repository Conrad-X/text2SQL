from enum import Enum

class DatabaseType(Enum):
    HOTEL = "hotel"
    STORE = "store"
    HEALTHCARE = "healthcare"

DATABASE_PATHS = {
    DatabaseType.HOTEL: "./databases/hotel.db",
    DatabaseType.STORE: "./databases/store.db",
    DatabaseType.HEALTHCARE: "./databases/healthcare.db",
}
