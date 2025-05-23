import os
import sys
import unittest
from unittest.mock import MagicMock, mock_open, patch

# Append the project root directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from pathlib import Path

from utilities.config import PATH_CONFIG
from utilities.constants.bird_utils.indexing_constants import (DB_ID_KEY,
                                                               QUESTION_ID_KEY,
                                                               SCHEMA_USED,
                                                               SQL)
from utilities.constants.database_enums import DatasetType

from server.preprocess.prepare_sample_dataset import (add_schema_used,
                                                      copy_bird_train_file,
                                                      create_train_file,
                                                      get_train_data,
                                                      get_train_file_path)


class TestGetTrainFilePathExisting(unittest.TestCase):
    @patch('os.path.exists', return_value=True)
    @patch('os.makedirs')
    @patch('shutil.copyfile')
    @patch('server.preprocess.prepare_sample_dataset.add_sequential_ids_to_questions')
    def test_get_train_file_path_existing(self, mock_add_ids, mock_copyfile, mock_makedirs, mock_exists):
        PATH_CONFIG.processed_train_path = MagicMock(return_value='/path/to/train_file.json')
        result = get_train_file_path()
        self.assertEqual(result, '/path/to/train_file.json')
        mock_add_ids.assert_not_called()

class TestGetTrainFilePathNonExisting(unittest.TestCase):
    @patch('os.path.exists', return_value=False)
    @patch('os.makedirs')
    @patch('shutil.copyfile')
    @patch('server.preprocess.prepare_sample_dataset.add_sequential_ids_to_questions')
    def test_get_train_file_path_non_existing(self, mock_add_ids, mock_copyfile, mock_makedirs, mock_exists):
        PATH_CONFIG.processed_train_path = MagicMock(return_value='/path/to/train_file.json')
        PATH_CONFIG.sample_dataset_type = DatasetType.BIRD_TRAIN
        result = get_train_file_path()
        self.assertEqual(result, '/path/to/train_file.json')
        mock_add_ids.assert_called_once_with('/path/to/train_file.json')

class TestCreateTrainFile(unittest.TestCase):
    @patch('os.makedirs')
    @patch('shutil.copyfile')
    @patch('server.preprocess.prepare_sample_dataset.add_sequential_ids_to_questions')
    def test_create_train_file(self, mock_add_ids, mock_copyfile, mock_makedirs):
        PATH_CONFIG.sample_dataset_type = DatasetType.BIRD_TRAIN

        # Test normal behavior
        create_train_file('/path/to/train_file.json')
        mock_makedirs.assert_called_once_with('/path/to', exist_ok=True)
        mock_copyfile.assert_called_once()
     
    @patch('os.makedirs')
    @patch('shutil.copyfile')
    @patch('server.preprocess.prepare_sample_dataset.add_sequential_ids_to_questions')   
    def test_file_not_found_error(self, mock_add_ids, mock_copyfile, mock_makedirs):
        """Test FileNotFoundError"""
        mock_copyfile.side_effect = FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            create_train_file('/path/to/train_file.json')
            
    @patch('os.makedirs')
    @patch('shutil.copyfile')
    @patch('server.preprocess.prepare_sample_dataset.add_sequential_ids_to_questions')
    def test_io_error(self, mock_add_ids, mock_copyfile, mock_makedirs):
        """Test IOError"""
        mock_copyfile.side_effect = IOError
        with self.assertRaises(IOError):
            create_train_file('/path/to/train_file.json')

    @patch('os.makedirs')
    @patch('shutil.copyfile')
    @patch('server.preprocess.prepare_sample_dataset.add_sequential_ids_to_questions')
    def test_permission_error(self, mock_add_ids, mock_copyfile, mock_makedirs):
        """Test PermissionError"""
        mock_copyfile.side_effect = PermissionError
        with self.assertRaises(PermissionError):
            create_train_file('/path/to/train_file.json')

class TestCreateTrainFilePermissionError(unittest.TestCase):
    @patch('shutil.copyfile')
    @patch('os.makedirs', side_effect=PermissionError("Permission denied"))
    def test_create_train_file_permission_error(self, mock_makedirs, mock_copyfile):
        PATH_CONFIG.sample_dataset_type = DatasetType.BIRD_TRAIN
        with self.assertRaises(PermissionError):
            create_train_file('/path/to/train_file.json')

class TestCopyBirdTrainFile(unittest.TestCase):
    @patch('shutil.copyfile')
    def test_copy_bird_train_file(self, mock_copyfile):
        PATH_CONFIG.bird_file_path = MagicMock(return_value='/path/to/source_file.json')
        copy_bird_train_file('/path/to/train_file.json')
        mock_copyfile.assert_called_once_with('/path/to/source_file.json', '/path/to/train_file.json')

class TestGetTrainDataValid(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open, read_data='{"db_id": "db1", "question_id": "q1", "sql": "SELECT * FROM table"}')
    @patch('os.path.exists', return_value=True)
    @patch('server.preprocess.prepare_sample_dataset.load_json_from_file', return_value=[{'db_id': 'db1', 'question_id': 'q1', 'sql': 'SELECT * FROM table'}])
    def test_get_train_data_valid(self, mock_open_file, mock_exists, mock_load_json):
        result = get_train_data('/path/to/train_file.json')
        self.assertEqual(result, [{'db_id': 'db1', 'question_id': 'q1', 'sql': 'SELECT * FROM table'}])

class TestGetTrainDataInvalidFile(unittest.TestCase):
    @patch('os.path.exists', return_value=False)
    def test_get_train_data_invalid_file(self, mock_exists):
        train_file = "/path/to/non_existent_file.json"
        result = get_train_data(train_file)
        self.assertIsNone(result)

class TestAddSchemaUsed(unittest.TestCase):
    @patch('server.preprocess.prepare_sample_dataset.save_json_to_file')
    @patch('server.preprocess.prepare_sample_dataset.get_sql_columns_dict', return_value={'columns': ['col1', 'col2']})
    @patch('builtins.open', new_callable=mock_open, read_data='{"db_id": "db1", "question_id": "q1", "sql": "SELECT * FROM table"}')
    def test_add_schema_used(self, mock_open_file, mock_get_sql_columns_dict, mock_save_json):
        train_data = [{DB_ID_KEY: 'db1', QUESTION_ID_KEY: 'q1', SQL: 'SELECT * FROM table'}]
        train_file = Path('/path/to/train_file.json')
        
        add_schema_used(train_data, DatasetType.BIRD_TRAIN, train_file)
        self.assertEqual(train_data[0][SCHEMA_USED], {'columns': ['col1', 'col2']})
        mock_save_json.assert_called_once()
        
    @patch('server.preprocess.prepare_sample_dataset.save_json_to_file')
    @patch('server.preprocess.prepare_sample_dataset.get_sql_columns_dict', side_effect=KeyboardInterrupt)
    @patch('builtins.open', new_callable=mock_open, read_data='{"db_id": "db1", "question_id": "q1", "sql": "SELECT * FROM table"}')
    def test_add_schema_used_keyboard_interrupt(self, mock_open_file, mock_get_sql_columns_dict, mock_save_json):
        train_data = [{DB_ID_KEY: 'db1', QUESTION_ID_KEY: 'q1', SQL: 'SELECT * FROM table'}]
        train_file = Path('/path/to/train_file.json')

        with self.assertRaises(KeyboardInterrupt):
            add_schema_used(train_data, DatasetType.BIRD_TRAIN, train_file)

        self.assertNotIn(SCHEMA_USED, train_data[0])

if __name__ == '__main__':
    unittest.main()