import sys
import os
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ETL.db import get_connection
from ETL.config import DW_DB, BATCH_SIZE
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)

def load_fact_transactions(df):
    try:
        conn = get_connection(DW_DB)
        cur = conn.cursor()
        logger.debug("Connexion au DW établie pour fact_transactions")

        logger.info("  → Chargement des mappings de dimensions...")
        account_map, type_map, time_map = load_dimension_maps(cur)
        logger.info(f"  ✅ Mappings chargés: {len(account_map)} comptes, {len(type_map)} types, {len(time_map)} pas de temps")

        query = """
        INSERT INTO fact_transactions (
            time_sk, origin_account_sk, destination_account_sk, transaction_type_sk,
            amount, origin_balance_before, origin_balance_after,
            destination_balance_before, destination_balance_after,
            is_fraud, is_flagged_fraud
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        total_rows = len(df)
        batch = []
        batch_count = 0
        total_inserted = 0

        logger.info(f"  → Début du chargement de {total_rows:,} transactions (batch size: {BATCH_SIZE:,})")
        
        for idx, r in enumerate(df.itertuples(index=False), 1):
            batch.append((
                time_map[r.step],
                account_map[r.origin_account],
                account_map[r.destination_account],
                type_map[r.transaction_type],
                r.amount,
                r.origin_balance_before,
                r.origin_balance_after,
                r.destination_balance_before,
                r.destination_balance_after,
                r.is_fraud,
                r.is_flagged_fraud
            ))

            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, query, batch)
                conn.commit()
                batch_count += 1
                total_inserted += len(batch)
                logger.info(f"  → Batch {batch_count} inséré: {len(batch):,} transactions ({total_inserted:,}/{total_rows:,} - {total_inserted/total_rows*100:.1f}%)")
                batch.clear()

        if batch:
            execute_batch(cur, query, batch)
            conn.commit()
            batch_count += 1
            total_inserted += len(batch)
            logger.info(f"  → Batch final {batch_count} inséré: {len(batch):,} transactions")

        logger.info(f"  ✅ {total_inserted:,} transactions chargées dans fact_transactions ({batch_count} batches)")
        
        conn.close()
        logger.debug("Connexion fermée")
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement de fact_transactions: {str(e)}")
        raise

def load_dimension_maps(cur):
    logger.debug("Chargement du mapping account_id -> account_sk...")
    cur.execute("SELECT account_id, account_sk FROM dim_account")
    account_map = dict(cur.fetchall())
    logger.debug(f"  → {len(account_map)} comptes mappés")

    logger.debug("Chargement du mapping transaction_type -> transaction_type_sk...")
    cur.execute("SELECT transaction_type, transaction_type_sk FROM dim_transaction_type")
    type_map = dict(cur.fetchall())
    logger.debug(f"  → {len(type_map)} types mappés")

    logger.debug("Chargement du mapping step -> time_sk...")
    cur.execute("SELECT step, time_sk FROM dim_time")
    time_map = dict(cur.fetchall())
    logger.debug(f"  → {len(time_map)} pas de temps mappés")

    return account_map, type_map, time_map
