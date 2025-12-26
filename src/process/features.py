from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Dict, Any
import pandas as pd

from src.utils.custom_logger import get_logger
from src.utils.custom_exception import CustomException


logger = get_logger(__name__)


class FeaturesCreation:

    def __init__(self, df : pd.DataFrame):
        self.df = df
        # certifies that the df is sorted by date
        self.df = self.df.sort_values('report_date')

        # creates a groupby cnpjs
        self.group_by_cnpj = self.df.groupby('fund_cnpj')



    def run(self):
        logger.debug("Starting FeaturesCreation.run()")

        
        self._add_return()
        self._add_gross_by_net()

        return self.df.reset_index(drop=True)
    
    def _add_return(self):
        logger.info("Creating 'return' feature")
        self.df['return'] = self.group_by_cnpj['quota_value'].pct_change()
        return self.df
    
    def _add_gross_by_net(self):
        logger.info("Creating 'gross_by_net' feature")
        self.df['gross_by_net'] = self.df['total_value'] / self.df['net_asset_value']
        return self.df
    
    def _add_feature3(self):
        logger.info("Creating '' feature")