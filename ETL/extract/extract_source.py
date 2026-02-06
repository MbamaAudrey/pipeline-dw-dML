import pandas as pd
import sys
import os
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ETL.db import get_connection
from ETL.config import SRC_DB

logger = logging.getLogger(__name__)

def extract_transactions():
    logger.info(f"Connexion à la base source: {SRC_DB['host']}:{SRC_DB['port']}/{SRC_DB['dbname']}")
    try:
        conn = get_connection(SRC_DB)
        logger.debug("Connexion établie avec succès")
        
        query = "SELECT * FROM transactions"
        logger.debug(f"Exécution de la requête: {query}")
        
        df = pd.read_sql(query, conn)
        logger.info(f"✅ Extraction réussie: {len(df):,} lignes, {len(df.columns)} colonnes")
        logger.debug(f"Colonnes extraites: {', '.join(df.columns.tolist())}")
        
        conn.close()
        logger.debug("Connexion fermée")
        
        return df
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'extraction: {str(e)}")
        raise
