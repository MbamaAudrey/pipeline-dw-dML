import sys
import os
import logging
from ETL.db import get_connection
from ETL.config import DW_DB
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logger = logging.getLogger(__name__)

# Traiter par chunks pour ne jamais charger tous les comptes en mémoire
CHUNK_ROWS = 50_000       # lignes de transactions par chunk
INSERT_BATCH = 1_000       # comptes par INSERT batch (executemany)

def load_dim_account(df):
    logger.info("→ Chargement de dim_account (mode flux : pas de liste complète en mémoire)...")
    try:
        conn = get_connection(DW_DB)
        cur = conn.cursor()

        total_rows = len(df)
        num_chunks = (total_rows + CHUNK_ROWS - 1) // CHUNK_ROWS
        logger.info(f"  → {total_rows:,} lignes → {num_chunks} chunks de {CHUNK_ROWS:,} lignes")

        total_accounts_inserted = 0
        for chunk_start in tqdm(
            range(0, total_rows, CHUNK_ROWS),
            desc="Chunks",
            unit="chunk",
            total=num_chunks,
            mininterval=0.5,
        ):
            chunk = df.iloc[chunk_start : chunk_start + CHUNK_ROWS]
            # Uniquement les comptes de ce chunk (mémoire limitée)
            accounts_chunk = set(chunk["origin_account"].dropna().astype(str)) | set(
                chunk["destination_account"].dropna().astype(str)
            )
            if not accounts_chunk:
                continue

            accounts_list = list(accounts_chunk)
            for i in range(0, len(accounts_list), INSERT_BATCH):
                batch = accounts_list[i : i + INSERT_BATCH]
                cur.executemany(
                    """
                    INSERT INTO dim_account (account_id)
                    VALUES (%s)
                    ON CONFLICT (account_id) DO NOTHING
                    """,
                    [(acc,) for acc in batch],
                )
            conn.commit()
            total_accounts_inserted += len(accounts_list)
            logger.info(
                f"  ✅ Chunk {chunk_start // CHUNK_ROWS + 1}/{num_chunks} "
                f"(lignes {chunk_start:,}-{min(chunk_start + CHUNK_ROWS, total_rows):,}) "
                f"— {len(accounts_list):,} comptes uniques dans ce chunk"
            )

        conn.close()
        logger.info(f"🎉 Chargement dim_account terminé (traitement par flux, pas de saturation mémoire)")

    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement de dim_account: {str(e)}")
        raise
