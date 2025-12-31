from src.config.settings import (
DATA_TRAIN_PATH_WITH_FEATURES,
DATA_TEST_PATH_WITH_FEATURES,
DATA_VALIDATION_PATH_WITH_FEATURES)
import pandas as pd

def load_data_with_features():
    df_train = pd.read_parquet(DATA_TRAIN_PATH_WITH_FEATURES)
    df_test = pd.read_parquet(DATA_TEST_PATH_WITH_FEATURES)
    df_val = pd.read_parquet(DATA_VALIDATION_PATH_WITH_FEATURES)

    return df_train, df_test, df_val