"""
URLs and constants for CVM data sources.
"""

# CVM Data URLs
BASE_URL = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
HIST_BASE_URL = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/"
FUND_CATALOG_URL = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"

# File naming patterns
MONTHLY_FILE_PATTERN = "inf_diario_fi_{year_month}.zip"
MONTHLY_CSV_PATTERN = "inf_diario_fi_{year_month}.csv"

# Column mappings
COLUMN_NAMES = {
    "cnpj": "CNPJ_FUNDO",
    "date": "DT_COMPTC",
    "quota": "VL_QUOTA",
    "net_assets": "VL_PATRIM_LIQ",
    "shareholders": "NR_COTST"
}

# Expected columns in monthly files
EXPECTED_COLUMNS = ["CNPJ_FUNDO", "DT_COMPTC", "VL_QUOTA", "VL_PATRIM_LIQ", "NR_COTST"]

# Fund catalog columns
FUND_CATALOG_COLUMNS = [
    'CNPJ_FUNDO', 'DENOM_SOCIAL', 'SIT', 'DT_REG', 'DT_CONST',
    'CD_CVM', 'DT_CANCEL', 'TP_FUNDO', 'DT_INI_SIT'
]