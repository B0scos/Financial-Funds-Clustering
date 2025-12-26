from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent.absolute()


## data pathes
DATA_RAW_ZIP_PATH = PROJECT_ROOT.parent / "data" / "raw_zip"
DATA_RAW_UNZIP_PATH = PROJECT_ROOT.parent / "data" / "raw_unzip"
DATA_PROCESSED_PATH = PROJECT_ROOT.parent / "data" / "processed"
DATA_INTERIM_PATH = PROJECT_ROOT.parent / "data" / "interim"


## data spliting options
data_split_cutoff = '2025-06-01'
train_test_split_ratio = 0.2
