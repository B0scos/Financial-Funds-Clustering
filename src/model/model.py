import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import RobustScaler
from src.utils.custom_logger import get_logger

logger = get_logger(__name__)


class PCAWrapper:
    """
    Wrapper for performing PCA consistently on train / test / validation sets.

    Parameters
    ----------
    train_df : pd.DataFrame
        Training dataframe used to fit PCA.
    test_df : pd.DataFrame
        Test dataframe transformed using the PCA fitted on train.
    val_df : pd.DataFrame
        Validation dataframe transformed using the PCA fitted on train.
    **pca_kwargs :
        Optional keyword arguments passed directly to sklearn.decomposition.PCA.
        If none are provided, PCA() uses sklearn defaults.

    Methods
    -------
    fit_transform():
        Fits PCA on train and transforms train, test and validation.
        Returns (train_pca, test_pca, val_pca)

    """

    def __init__(
        self,
        train_df: pd.DataFrame,
        test_df: pd.DataFrame,
        val_df: pd.DataFrame,
        **pca_kwargs
    ):
        self.train_df = train_df
        self.test_df = test_df
        self.val_df = val_df

        # args are optional – this is the key point
        self.pca = PCA(**pca_kwargs)

    def fit_transform(self):
        logger.info("Starting PCA fit_transform")
        train = self.pca.fit_transform(self.train_df)
        test = self.pca.transform(self.test_df)
        val = self.pca.transform(self.val_df)
        return train, test, val



class RobustScalerWrapper:
    """
    Wrapper for applying RobustScaler consistently on
    train / test / validation ensuring no data leakage.

    Parameters
    ----------
    train_df : pd.DataFrame
        Data used to fit the scaler.
    test_df : pd.DataFrame
        Data transformed using scaler fit on train.
    val_df : pd.DataFrame
        Data transformed using scaler fit on train.
    **scaler_kwargs :
        Optional keyword arguments passed to sklearn.preprocessing.RobustScaler.

    Methods
    -------
    fit_transform():
        Fits scaler on train and transforms all datasets.
        Returns (train_scaled, test_scaled, val_scaled)
    """

    def __init__(
        self,
        train_df: pd.DataFrame,
        test_df: pd.DataFrame,
        val_df: pd.DataFrame,
        **scaler_kwargs
    ):
        self.train_df = train_df
        self.test_df = test_df
        self.val_df = val_df
        self.scaler = RobustScaler(**scaler_kwargs)

    def fit_transform(self):
        logger.info("Starting RobustScaler fit_transform")
        train = self.scaler.fit_transform(self.train_df)
        test = self.scaler.transform(self.test_df)
        val = self.scaler.transform(self.val_df)
        return train, test, val
