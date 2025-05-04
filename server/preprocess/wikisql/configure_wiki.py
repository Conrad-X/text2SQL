import os
import sys
import shutil
import subprocess
import urllib.request
import tempfile
from pathlib import Path

# GitHub raw content URL for the data.tar.bz2 file
WIKISQL_DATA_URL = "https://github.com/salesforce/WikiSQL/raw/master/data.tar.bz2"

# Define paths directly without importing PATH_CONFIG (which fails on initialization)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = REPO_ROOT / "data" / "wikisql"

# Define dataset directories
DEV_DATASET_DIR = DATA_DIR / "dev_dataset"
TEST_DATASET_DIR = DATA_DIR / "test_dataset"

# Define database directories
DEV_DB_DIR = DEV_DATASET_DIR / "dev"
TEST_DB_DIR = TEST_DATASET_DIR / "test"

def create_directory_structure():
    """Create the necessary directory structure"""
    print("Creating directory structure...")
    
    # Create all directories
    for directory in [DATA_DIR, DEV_DATASET_DIR, TEST_DATASET_DIR, DEV_DB_DIR, TEST_DB_DIR]:
        os.makedirs(directory, exist_ok=True)
        
    print(f"Directory structure created at {DATA_DIR}")

def download_wikisql_data(url, target_file):
    """Download the WikiSQL dataset"""
    print(f"Downloading WikiSQL dataset from {url}...")
    
    try:
        # Create a progress bar for download
        def report_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(100, downloaded * 100 / total_size)
            sys.stdout.write(f"\rProgress: {percent:.1f}% ({downloaded/1024/1024:.1f} MB)")
            sys.stdout.flush()
        
        # Download the file
        urllib.request.urlretrieve(url, target_file, report_progress)
        print("\nDownload complete.")
        return True
    
    except Exception as e:
        print(f"\nError downloading file: {e}")
        return False

def extract_tar_file(tar_file, extract_dir):
    """Extract the tar.bz2 file"""
    print(f"Extracting {tar_file} to {extract_dir}...")
    
    try:
        # Use tar command through subprocess
        result = subprocess.run(
            ["tar", "xvjf", tar_file, "-C", extract_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode == 0:
            print("Extraction complete.")
            return True
        else:
            print(f"Error during extraction: {result.stderr}")
            return False
    
    except Exception as e:
        print(f"Error extracting file: {e}")
        return False

def organize_files(extracted_dir):
    """Organize the files into the proper structure"""
    print("Organizing files into the proper structure...")
    
    # Define file mappings (source -> destination)
    file_mappings = {
        # Dev dataset files
        os.path.join(extracted_dir, "data", "dev.db"): 
            os.path.join(DEV_DB_DIR, "dev.db"),
        os.path.join(extracted_dir, "data", "dev.jsonl"): 
            os.path.join(DEV_DATASET_DIR, "dev.jsonl"),
        os.path.join(extracted_dir, "data", "dev.tables.jsonl"): 
            os.path.join(DEV_DATASET_DIR, "dev.tables.jsonl"),
        
        # Test dataset files
        os.path.join(extracted_dir, "data", "test.db"): 
            os.path.join(TEST_DB_DIR, "test.db"),
        os.path.join(extracted_dir, "data", "test.jsonl"): 
            os.path.join(TEST_DATASET_DIR, "test.jsonl"),
        os.path.join(extracted_dir, "data", "test.tables.jsonl"): 
            os.path.join(TEST_DATASET_DIR, "test.tables.jsonl"),
    }
    
    # Move each file to its destination
    for src, dest in file_mappings.items():
        if os.path.exists(src):
            print(f"Moving {os.path.basename(src)} to {dest}")
            shutil.copy2(src, dest)
        else:
            print(f"Warning: Source file {src} not found")
    
    print("Files organized successfully.")

def setup_wikisql():
    """Main function to set up the WikiSQL dataset"""
    print("Starting WikiSQL dataset setup...")

    # Create the directory structure
    create_directory_structure()
    
    # Create a temporary directory for download and extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        # Define file paths
        tar_file = os.path.join(temp_dir, "data.tar.bz2")
        extract_dir = os.path.join(temp_dir, "extract")
        os.makedirs(extract_dir, exist_ok=True)
        
        # Download the dataset
        if not download_wikisql_data(WIKISQL_DATA_URL, tar_file):
            print("Failed to download the WikiSQL dataset.")
            return False
        
        # Extract the tar file
        if not extract_tar_file(tar_file, extract_dir):
            print("Failed to extract the WikiSQL dataset.")
            return False
        
        # Organize the files
        organize_files(extract_dir)
    
    print("\nWikiSQL dataset setup complete!")
    print(f"Dev dataset located at: {DEV_DATASET_DIR}")
    print(f"Test dataset located at: {TEST_DATASET_DIR}")
    
    
    # Print instructions for running the next script
    print("\nRun the next script to create the converted JSON datasets:")
    print("python3 -m preprocess.wikisql.convert_wiki --dataset dev")
    print("python3 -m preprocess.wikisql.convert_wiki --dataset test")
    print("python3 -m preprocess.wikisql.convert_wiki --dataset all")

    
    return True

if __name__ == "__main__":
    """
    WikiSQL Dataset Setup Script

    This script downloads the WikiSQL dataset, extracts it, and organizes it
    into the proper folder structure for the text2SQL project.

    Usage:
        python3 -m preprocess.wikisql.configure_wiki

    The script should be run from the server directory.
    """
    setup_wikisql()