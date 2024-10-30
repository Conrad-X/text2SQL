from enum import Enum

class DatabaseType(Enum):
    HOTEL = "hotel"
    STORE = "store"
    HEALTHCARE = "healthcare"
    MUSICFESTIVAL = "music_festival"
    FORMULA1 = "formula_1"

DATABASE_PATHS = {
    DatabaseType.HOTEL: "./databases/hotel.db",
    DatabaseType.STORE: "./databases/store.db",
    DatabaseType.HEALTHCARE: "./databases/healthcare.db",
    DatabaseType.MUSICFESTIVAL: "./databases/music_festival.db",
    DatabaseType.FORMULA1: "./databases/formula_1.db"
}
