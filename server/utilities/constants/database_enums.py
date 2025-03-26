from enum import Enum

# DatabaseType and Paths are for synthetic dataset only
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

class DatasetType(Enum):
    BIRD_TRAIN = "bird_train"
    BIRD_DEV = "bird_dev"
    BIRD_TEST = "bird_test"
    SYNTHETIC = "synthetic"
    WIKI_DEV = "wiki_dev"
    WIKI_TEST = "wiki_test"
