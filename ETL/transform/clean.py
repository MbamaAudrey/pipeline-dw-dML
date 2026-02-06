import logging

logger = logging.getLogger(__name__)

def clean_data(df):
    initial_count = len(df)
    initial_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
    
    logger.debug(f"Données avant nettoyage: {initial_count:,} lignes, {initial_memory:.2f} MB")
    
    # Suppression des doublons
    duplicates_before = df.duplicated().sum()
    df = df.drop_duplicates()
    duplicates_removed = initial_count - len(df)
    logger.info(f"  → Doublons supprimés: {duplicates_removed:,} lignes")
    
    # Gestion des valeurs manquantes
    nulls_before = df.isnull().sum().sum()
    df = df.fillna(0)
    nulls_filled = nulls_before
    logger.info(f"  → Valeurs nulles remplacées: {nulls_filled:,} valeurs")
    
    final_count = len(df)
    final_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB

    logger.debug(f"Données après nettoyage: {final_count:,} lignes, {final_memory:.2f} MB")
    removed = initial_count - final_count
    pct = (removed / initial_count * 100) if initial_count else 0
    logger.info(f"  → Réduction: {removed:,} lignes supprimées ({pct:.2f}%)")

    return df
