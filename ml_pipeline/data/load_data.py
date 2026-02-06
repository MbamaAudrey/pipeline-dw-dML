import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _root not in sys.path:
    sys.path.insert(0, _root)

import pandas as pd
from ETL.db import get_connection
from ETL.config import DW_DB

def load_ml_mart():
    conn = get_connection(DW_DB)
    query = "SELECT * FROM transactions_ml_mart"
    df = pd.read_sql(query, conn)
    conn.close()
    return df
