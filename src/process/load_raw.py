import pandas as pd
import os
from src.config.settings import DATA_RAW_UNZIP_PATH, DATA_PROCESSED_PATH


class ProcessRaw():
    
    def __init__(self):
         self.path_raw_data = DATA_RAW_UNZIP_PATH
         self.path_processed_path = DATA_PROCESSED_PATH
    
    def concat(self):
        df_arr = []
        for root, dirs, files in os.walk(self.path_raw_data):
            for file in files:
                file_path = os.path.join(root, file)

                df_read = pd.read_csv(file_path, sep=';')

                print(df_read.columns)
                
                df_arr.append(df_read)
            
        df = pd.concat(df_arr)

        return df


