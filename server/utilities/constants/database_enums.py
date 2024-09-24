from enum import Enum

class DatabaseType(Enum):
    HOTEL = "hotel"
    STORE = "store"
    HEALTHCARE = "healthcare"
    MUSICFESTIVAL = "music_festival"

DATABASE_PATHS = {
    DatabaseType.HOTEL: "./databases/hotel.db",
    DatabaseType.STORE: "./databases/store.db",
    DatabaseType.HEALTHCARE: "./databases/healthcare.db",
    DatabaseType.MUSICFESTIVAL: "./databases/music_festival.db",
}
