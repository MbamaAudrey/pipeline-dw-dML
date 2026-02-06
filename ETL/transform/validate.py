import logging

logger = logging.getLogger(__name__)

def validate_data(df):
    logger.debug("Début de la validation des données...")
    
    # Validation des montants
    negative_amounts = (df["amount"] < 0).sum()
    if negative_amounts > 0:
        logger.error(f"❌ {negative_amounts} montants négatifs détectés")
        raise ValueError("Montant négatif détecté")
    logger.info(f"  → Validation montants: {len(df):,} montants valides (>= 0)")
    
    # Validation des comptes d'origine
    null_origin = df["origin_account"].isnull().sum()
    if null_origin > 0:
        logger.error(f"❌ {null_origin} comptes d'origine nuls détectés")
        raise ValueError("Compte d'origine nul détecté")
    logger.info(f"  → Validation origin_account: {len(df):,} comptes valides")
    
    # Validation des comptes de destination
    null_dest = df["destination_account"].isnull().sum()
    if null_dest > 0:
        logger.error(f"❌ {null_dest} comptes de destination nuls détectés")
        raise ValueError("Compte de destination nul détecté")
    logger.info(f"  → Validation destination_account: {len(df):,} comptes valides")
    
    logger.debug("Validation terminée avec succès")
