import sys
import os
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ETL.db import get_connection
from ETL.config import DW_DB

logger = logging.getLogger(__name__)

# Nombre total de lignes cible dans le data mart ML
ML_MART_MAX_ROWS = 30_000

def build_ml_mart():
    try:
        conn = get_connection(DW_DB)
        cur = conn.cursor()
        logger.debug("Connexion au DW établie pour build_ml_mart")

        logger.info("  → Suppression de l'ancienne table transactions_ml_mart si elle existe...")
        cur.execute("DROP TABLE IF EXISTS transactions_ml_mart;")
        logger.debug("  → Ancienne table supprimée")

        # Nombre de fraudes dans la fact pour dimensionner l'échantillon non-fraude
        cur.execute("SELECT COUNT(*) FROM fact_transactions WHERE is_fraud = true")
        fraud_count = cur.fetchone()[0]
        non_fraud_limit = max(0, ML_MART_MAX_ROWS - fraud_count)
        logger.info(f"  → Fraudes à conserver: {fraud_count:,} | Non-fraudes à échantillonner: {non_fraud_limit:,}")

        logger.info("  → Création de la table transactions_ml_mart (toutes les fraudes + échantillon non-fraudes)...")
        base_cols = """
            f.amount,
            f.origin_balance_before - f.origin_balance_after AS origin_balance_diff,
            f.destination_balance_after - f.destination_balance_before AS dest_balance_diff,
            tt.transaction_type,
            f.is_fraud AS label
        """
        # Toutes les fraudes + échantillon aléatoire de non-fraudes (LIMIT appliqué uniquement à la 2e partie)
        cur.execute(f"""
        CREATE TABLE transactions_ml_mart AS
        SELECT {base_cols.strip()}
        FROM fact_transactions f
        JOIN dim_transaction_type tt ON f.transaction_type_sk = tt.transaction_type_sk
        WHERE f.is_fraud = true
        UNION ALL
        SELECT * FROM (
            SELECT {base_cols.strip()}
            FROM fact_transactions f
            JOIN dim_transaction_type tt ON f.transaction_type_sk = tt.transaction_type_sk
            WHERE f.is_fraud = false
            ORDER BY RANDOM()
            LIMIT %s
        ) non_fraud_sample;
        """, (non_fraud_limit,))

        # Compter les lignes créées
        cur.execute("SELECT COUNT(*) FROM transactions_ml_mart")
        row_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM transactions_ml_mart WHERE label = true")
        fraud_in_mart = cur.fetchone()[0]

        conn.commit()
        logger.info(f"  ✅ Table transactions_ml_mart créée: {row_count:,} lignes")
        pct_fraud = (fraud_in_mart / row_count * 100) if row_count else 0
        logger.info(f"  → Statistiques: {fraud_in_mart:,} fraudes ({pct_fraud:.2f}%), {row_count - fraud_in_mart:,} transactions normales")

        conn.close()
        logger.debug("Connexion fermée")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la construction du data mart ML: {str(e)}")
        raise
