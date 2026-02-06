import sys
import os
import logging
from ETL.db import get_connection
from ETL.config import DW_DB, BATCH_SIZE

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logger = logging.getLogger(__name__)

def load_dim_time(df):
    try:
        conn = get_connection(DW_DB)
        cur = conn.cursor()
        logger.debug("Connexion au DW établie pour dim_time")

        steps = sorted(df["step"].dropna().unique().tolist())
        if not steps:
            logger.warning("  → Aucun pas de temps à charger (colonne step vide).")
            conn.close()
            return

        logger.info(f"  → {len(steps):,} pas de temps uniques à charger (min: {min(steps)}, max: {max(steps)})")

        # Charger par batch
        inserted = 0
        for i in range(0, len(steps), BATCH_SIZE):
            batch = steps[i:i+BATCH_SIZE]
            cur.executemany("""
                INSERT INTO dim_time (step)
                VALUES (%s)
                ON CONFLICT DO NOTHING
            """, [(int(s),) for s in batch])
            conn.commit()
            inserted += cur.rowcount
            logger.info(f"  ✅ Batch {i//BATCH_SIZE + 1}: {len(batch):,} pas de temps chargés")

        conn.close()
        logger.debug("Connexion fermée")
        logger.info(f"  🎉 Tous les pas de temps ont été chargés avec succès")
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement de dim_time: {str(e)}")
        raise
