import pandas as pd
from src.process.load_raw import ProcessRaw
from src.process.clean_data import DataCleaner, DataCleanerConfig
from src.process.features import FeaturesCreation
from src.utils.utils import data_spliter, save_dataframe_parquet
from src.utils.custom_exception import CustomException
from src.utils.custom_logger import get_logger
from src.config.settings import (
    DATA_INTERIM_PATH,
    DATA_TRAIN_PATH,
    DATA_TEST_PATH,
    DATA_VALIDATION_PATH,
    DATA_TRAIN_PATH_WITH_FEATURES,
    DATA_TEST_PATH_WITH_FEATURES,
    DATA_VALIDATION_PATH_WITH_FEATURES,
    train_test_split_ratio,
    data_split_cutoff,
    train_test_cutoff
)

logger = get_logger(__name__)


def data_pipeline(rebuild_interim: bool = False):
    """
    Full data pipeline:
      1) Load + concat raw data (optional)
      2) Clean
      3) Split train/test/val
      4) Feature engineering
      5) Save outputs

    Parameters
    ----------
    rebuild_interim : bool, default=False
        If True, rebuilds interim dataset from raw sources.
        If False, reads existing interim parquet.
    """

    try:
        logger.info("Starting data pipeline")

        # ----- Load / Clean -----
        if rebuild_interim:
            logger.info("Rebuilding interim dataset...")
            pr = ProcessRaw()
            df = pr.concat()
            pr.save(df, filename="interim.parquet", fmt="parquet", target="interim")

            cfg = DataCleanerConfig(required_columns=["fund_cnpj", "report_date"])
            cleaner = DataCleaner(config=cfg)

            cleaned = cleaner.run(
                df,
                save=True,
                filename="cleaned.parquet",
                fmt="parquet"
            )
        else:
            logger.info("Loading existing interim dataset...")
            cleaned = pd.read_parquet(DATA_INTERIM_PATH)

        if cleaned.empty:
            raise ValueError("Cleaned dataframe is empty. Pipeline aborted.")

        logger.info(f"Dataset size after cleaning: {len(cleaned):,} rows")

        # ----- Split -----
        logger.info("Splitting dataset into Train / Test / Validation")
        train_df, test_df, val_df = data_spliter(
            df=cleaned,
            val_cutoff=data_split_cutoff,
            train_test_cutoff=train_test_cutoff,
            test_ratio=train_test_split_ratio
        )

        logger.info(
            f"Split sizes — Train: {len(train_df):,} | "
            f"Test: {len(test_df):,} | "
            f"Validation: {len(val_df):,}"
        )

        # ----- Feature Creation -----
        def run_features(df: pd.DataFrame, name: str):
            fc = FeaturesCreation(df)
            logger.info(f"Running feature creation for {name}")
            base = fc.run()
            agg = fc.aggregate_features()
            return base, agg

        train_base, train_agg = run_features(train_df, "TRAIN")
        test_base, test_agg = run_features(test_df, "TEST")
        val_base, val_agg = run_features(val_df, "VALIDATION")

        # ----- Save Base -----
        logger.info("Saving split datasets")
        save_dataframe_parquet(train_base, DATA_TRAIN_PATH)
        save_dataframe_parquet(test_base, DATA_TEST_PATH)
        save_dataframe_parquet(val_base, DATA_VALIDATION_PATH)

        # ----- Save Feature Aggregations -----
        logger.info("Saving datasets with aggregated features")
        save_dataframe_parquet(train_agg, DATA_TRAIN_PATH_WITH_FEATURES, index=True)
        save_dataframe_parquet(test_agg, DATA_TEST_PATH_WITH_FEATURES, index=True)
        save_dataframe_parquet(val_agg, DATA_VALIDATION_PATH_WITH_FEATURES, index=True)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.exception("Pipeline crashed")
        raise CustomException(f"data_pipeline failed: {e}") from e
