from enum import Enum

class DatabaseType(Enum):
    HOTEL = "hotel"
    STORE = "store"

DATABASE_PATHS = {
    DatabaseType.HOTEL: "./databases/hotel.db",
    DatabaseType.STORE: "./databases/store.db",
}
