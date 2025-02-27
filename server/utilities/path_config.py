from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from utilities.constants.database_enums import DatasetType


@dataclass
class PathConfig:
    dataset_type: DatasetType = DatasetType.BIRD_DEV
    sample_dataset_type: DatasetType = DatasetType.BIRD_DEV
    database_name: str = field(init=False)
    repo_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent
    )

    def __post_init__(self):
        self.database_name = next(
            (p.name for p in self.dataset_dir().iterdir() if p.is_dir()), None
        )

    def set_database(self, database_name: str):
        self.database_name = database_name

    def base_dir(self, dataset_type: Optional[DatasetType] = None) -> Path:
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type

        if dataset_type == DatasetType.BIRD_TRAIN:
            return self.repo_root / "data" / "bird" / "train"
        elif dataset_type == DatasetType.BIRD_DEV:
            return self.repo_root / "data" / "bird" / "dev_20240627" 
        elif dataset_type == DatasetType.SYNTHETIC:
            return self.repo_root 
        else:
            raise ValueError("Unknown dataset type.")

    def dataset_dir(self, dataset_type: Optional[DatasetType] = None) -> Path:
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type

        if dataset_type == DatasetType.BIRD_TRAIN:
            return self.base_dir(dataset_type=dataset_type) / "train_databases"
        elif dataset_type == DatasetType.BIRD_DEV:
            return self.base_dir(dataset_type=dataset_type) / "dev_databases"
        elif dataset_type == DatasetType.SYNTHETIC:
            return self.base_dir(dataset_type=dataset_type) / "databases"
        else:
            raise ValueError("Unknown dataset type.")

    def database_dir(self, database_name: Optional[str] = None, dataset_type: Optional[DatasetType] = None) -> Path:
        database_name = database_name if database_name is not None else self.database_name
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type

        if dataset_type == DatasetType.BIRD_TRAIN:
            return self.dataset_dir(dataset_type=dataset_type) / database_name
            
        elif dataset_type == DatasetType.BIRD_DEV:
            return self.dataset_dir(dataset_type=dataset_type) / database_name
        
        elif dataset_type == DatasetType.SYNTHETIC:
            return self.base_dir(dataset_type=dataset_type) / "databases"
        
        else:
            raise ValueError("Unknown dataset type.")

    def sqlite_path(self, database_name: Optional[str] = None, dataset_type: Optional[DatasetType] = None) -> Path:
        database_name = database_name if database_name is not None else self.database_name
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type

        if dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return self.database_dir(database_name=database_name, dataset_type=dataset_type) / f"{database_name}.sqlite"
        
        elif dataset_type == DatasetType.SYNTHETIC:
            return self.database_dir(database_name=database_name, dataset_type=dataset_type) / f"{database_name}.db"

    def processed_train_path(self, database_name: Optional[str] = None, global_file: Optional[bool] = False) -> Optional[Path]:
        if self.dataset_type not in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return None  # Only Support Bird Dataset
        
        if global_file:
            return self.base_dir(self.dataset_type) / "processed_train.json"
        
        if database_name:
            return self.database_dir(database_name=database_name) / f"processed_train_{database_name}.json"

        if self.sample_dataset_type == self.dataset_type:
            return self.database_dir(self.database_name) / f"processed_train_{self.database_name}.json"

        return self.base_dir(self.sample_dataset_type) / "processed_train.json"


    def processed_test_path(self, database_name: Optional[str] = None, global_file: Optional[bool] = False) -> Optional[Path]:
        if self.dataset_type not in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return None  # Only Support Bird Dataset
        
        if global_file:
            return self.base_dir(self.dataset_type) / "processed_test.json"
        
        if database_name:
            return self.database_dir(database_name=database_name) / f"processed_test_{database_name}.json"

        if self.sample_dataset_type == self.dataset_type:
            return self.database_dir(self.database_name) / f"processed_test_{self.database_name}.json"

        return self.base_dir(self.sample_dataset_type) / "processed_test.json"

    def formatted_predictions_path(self, database_name: Optional[str] = None, global_file: Optional[bool] = False) -> Optional[Path]:
        database_name = database_name if database_name is not None else self.database_name

        if global_file:
            if self.dataset_type == DatasetType.BIRD_DEV:   
                return self.base_dir(self.dataset_type) / "predict_dev.json"
            elif self.dataset_type == DatasetType.BIRD_TRAIN:
                return self.base_dir(self.dataset_type) / "predict_train.json"

        if self.dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return self.database_dir(database_name=database_name) / f"formatted_predictions_{database_name}.json"

        return None
    
    def test_gold_path(self, database_name: Optional[str] = None, global_file: Optional[bool] = False) -> Path:
        database_name = database_name if database_name is not None else self.database_name

        if global_file:
            if self.dataset_type == DatasetType.BIRD_DEV:
                return self.base_dir(self.dataset_type) / "dev_gold.sql"
            elif self.dataset_type == DatasetType.BIRD_TRAIN:
                return self.base_dir(self.dataset_type) / "train_gold.sql"

        if self.dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return self.database_dir(database_name=database_name) / f"test_gold_{database_name}.sql"

        return None

    def database_preprocessed_dir(self, database_name: Optional[str] = None) -> Path:
        database_name = database_name if database_name is not None else self.database_name

        if self.dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return self.database_dir(database_name=database_name) / "preprocessed"
        
        return None

    def unique_values_path(self, database_name: Optional[str] = None) -> Path:
        database_name = database_name if database_name is not None else self.database_name

        if self.dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return self.database_preprocessed_dir(database_name=database_name) / f"{database_name}_unique_values.pkl"

        return None

    def lsh_path(self, database_name: Optional[str] = None) -> Path:
        database_name = database_name if database_name is not None else self.database_name

        if self.dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return self.database_preprocessed_dir(database_name=database_name) / f"{database_name}_lsh.pkl"
        
        return None

    def minhashes_path(self, database_name: Optional[str] = None) -> Path:
        database_name = database_name if database_name is not None else self.database_name

        if self.dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return self.database_preprocessed_dir(database_name=database_name) / f"{database_name}_minhashes.pkl"

        return None

    def batch_input_path(self, database_name: Optional[str] = None) -> Path:
        database_name = database_name if database_name is not None else self.database_name

        if self.dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return (
                self.database_dir(database_name=database_name)
                / "batch_jobs"
                / f"batch_job_input_{database_name}.jsonl"
            )

        elif self.dataset_type == DatasetType.SYNTHETIC:
            return (
                self.repo_root
                / "data"
                / "batch_jobs"
                / "batch_input_files"
                / f"{database_name}_batch_job_input.jsonl"
            )

    def batch_output_path(self, database_name: Optional[str] = None) -> Path:
        database_name = database_name if database_name is not None else self.database_name

        if self.dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return (
                self.database_dir(database_name=database_name)
                / "batch_jobs"
                / f"batch_job_output_{database_name}.jsonl"
            )

        elif self.dataset_type == DatasetType.SYNTHETIC:
            return (
                self.repo_root
                / "data"
                / "batch_jobs"
                / "batch_input_files"
                / f"{database_name}_batch_job_output.jsonl"
            )

    def description_dir(self, database_name: Optional[str] = None, dataset_type: Optional[DatasetType] = None ) -> Path:
        database_name = database_name if database_name is not None else self.database_name
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type

        if dataset_type in (DatasetType.BIRD_TRAIN, DatasetType.BIRD_DEV):
            return self.database_dir(database_name=database_name, dataset_type=dataset_type) / "database_description"

        return None

    def bird_results_dir(self) -> Path:
        return self.repo_root / "bird_results"

    def batch_job_metadata_dir(self) -> Path:
        return self.repo_root / "batch_job_metadata"

    def bird_file_path(self, dataset_type: Optional[DatasetType] = None) -> Path:
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type

        if dataset_type == DatasetType.BIRD_DEV:
            return self.base_dir(dataset_type=dataset_type) / "dev.json"
        elif dataset_type == DatasetType.BIRD_TRAIN:
            return self.base_dir(dataset_type=dataset_type) / "train.json"

        return None

    def bird_schema_file_path(self, dataset_type: Optional[DatasetType] = None) -> Path:
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type

        if dataset_type == DatasetType.BIRD_DEV:
            return self.base_dir(dataset_type=dataset_type) / "dev_tables.json"

        elif dataset_type == DatasetType.BIRD_TRAIN:
            return self.base_dir(dataset_type=dataset_type) / "train_tables.json"

        return None

    def correct_generated_file(self, dataset_type: Optional[DatasetType] = None) -> Path:
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type
        return self.dataset_dir(dataset_type=dataset_type) / "correct_generated.csv"
    
    def config_selected_file(self, dataset_type: Optional[DatasetType] = None) -> Path:
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type
        return self.dataset_dir(dataset_type=dataset_type) / "config_selected.csv"

    def correct_selected_file(self, dataset_type: Optional[DatasetType] = None) -> Path:
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type
        return self.dataset_dir(dataset_type=dataset_type) / "correct_selected.csv"

    def refiner_data_file(self, dataset_type: Optional[DatasetType] = None) -> Path:
        dataset_type = dataset_type if dataset_type is not None else self.dataset_type
        return self.dataset_dir(dataset_type=dataset_type) / "refiner_data.csv"