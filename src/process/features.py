from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Dict, Any
import pandas as pd

from src.utils.custom_logger import get_logger
from src.utils.custom_exception import CustomException


logger = get_logger(__name__)


class FeaturesCreation:
    """
    Build financial features for funds dataframe.
    Assumes dataframe contains:
        - 'report_date'
        - 'fund_cnpj'
        - 'quota_value'
        - 'total_value'
        - 'net_asset_value'
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.sort_values("report_date")
        self.group_by_cnpj = self.df.groupby("fund_cnpj")

    def run(self) -> pd.DataFrame:
        logger.debug("Starting FeaturesCreation.run()")

        self._add_return()
        self._add_gross_by_net()
        self._add_vol_std([5, 10, 15, 22, 50], "return")

        return self.df.reset_index(drop=True)

    def _add_return(self) -> pd.DataFrame:
        logger.info("Creating 'return' feature")
        self.df["return"] = self.group_by_cnpj["quota_value"].pct_change()
        return self.df

    def _add_gross_by_net(self) -> pd.DataFrame:
        logger.info("Creating 'gross_by_net' feature")
        self.df["gross_by_net"] = self.df["total_value"] / self.df["net_asset_value"]
        return self.df

    def _add_vol_std(self, list_windows: list, col: str) -> pd.DataFrame:
        logger.info("Creating 'vol' features based on standard deviation")
        for win in list_windows:
            self.df[f"vol_{win}"] = self.df[col].rolling(win).std()
        return self.df
