import sys
import os
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ETL.db import get_connection
from ETL.config import DW_DB

logger = logging.getLogger(__name__)

def load_dim_transaction_type(df):
    logger.info(f"Connexion au DW: {DW_DB['host']}:{DW_DB['port']}/{DW_DB['dbname']}")
    try:
        conn = get_connection(DW_DB)
        cur = conn.cursor()
        logger.debug("Connexion au DW établie")

        types = list(set(df["transaction_type"].values))
        logger.info(f"  → {len(types)} types de transactions uniques à charger: {types}")

        cur.executemany("""
            INSERT INTO dim_transaction_type (transaction_type)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, [(t,) for t in types])

        conn.commit()
        logger.info(f"  ✅ {len(types)} types de transactions chargés dans dim_transaction_type")
        
        conn.close()
        logger.debug("Connexion fermée")
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement de dim_transaction_type: {str(e)}")
        raise
