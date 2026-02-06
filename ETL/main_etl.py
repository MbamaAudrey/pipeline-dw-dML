import sys
import os
import logging
from datetime import datetime

# Ajouter le répertoire parent au PYTHONPATH pour que les imports ETL fonctionnent
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ETL.extract.extract_source import extract_transactions
from ETL.transform.clean import clean_data
from ETL.transform.standardize import standardize_columns
from ETL.transform.validate import validate_data
from ETL.load.load_dim_transaction_type import load_dim_transaction_type
from ETL.load.load_dim_account import load_dim_account
from ETL.load.load_dim_time import load_dim_time
from ETL.load.load_fact_transactions import load_fact_transactions
from ETL.data_mart.build_ml_mart import build_ml_mart

# Configuration du logging
def setup_logging():
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'ETL/logs/etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    return logging.getLogger(__name__)

def main():
    # Créer le dossier logs s'il n'existe pas
    os.makedirs('ETL/logs', exist_ok=True)
    
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("🚀 DÉMARRAGE DU PIPELINE ETL")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    
    try:
        # EXTRACT
        logger.info("📥 [EXTRACT] Début de l'extraction des données source...")
        df = extract_transactions()
        logger.info(f"✅ [EXTRACT] {len(df):,} transactions extraites avec succès")
        
        # TRANSFORM
        logger.info("🔄 [TRANSFORM] Début de la transformation des données...")
        
        logger.info("  → Nettoyage des données...")
        initial_count = len(df)
        df = clean_data(df)
        logger.info(f"  ✅ Nettoyage terminé: {initial_count:,} → {len(df):,} lignes (doublons supprimés)")
        
        logger.info("  → Standardisation des colonnes...")
        df = standardize_columns(df)
        logger.info(f"  ✅ Standardisation terminée: {len(df.columns)} colonnes")
        
        logger.info("  → Validation des données...")
        validate_data(df)
        logger.info("  ✅ Validation réussie: toutes les données sont valides")
        
        logger.info(f"✅ [TRANSFORM] Transformation terminée: {len(df):,} lignes prêtes pour le chargement")
        
        # LOAD
        logger.info("📤 [LOAD] Début du chargement dans le Data Warehouse...")
        
        logger.info("  → Chargement de dim_transaction_type...")
        load_dim_transaction_type(df)
        logger.info("  ✅ dim_transaction_type chargé")
        
        logger.info("  → Chargement de dim_account...")
        load_dim_account(df) 
        logger.info("  ✅ dim_account chargé")
        
        logger.info("  → Chargement de dim_time...")
        load_dim_time(df)
        logger.info("  ✅ dim_time chargé")
        
        logger.info("  → Chargement de fact_transactions...")
        load_fact_transactions(df)
        logger.info("  ✅ fact_transactions chargé")
        
        logger.info("✅ [LOAD] Chargement terminé avec succès")
        
        # DATA MART
        logger.info("📊 [DATA MART] Construction du data mart ML...")
        build_ml_mart()
        logger.info("✅ [DATA MART] Data mart ML construit avec succès")
        
        # Résumé final
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info("✅ PIPELINE ETL TERMINÉ AVEC SUCCÈS")
        logger.info(f"⏱️  Durée totale: {duration:.2f} secondes")
        logger.info(f"📊 {len(df):,} transactions traitées")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ ERREUR CRITIQUE DANS LE PIPELINE ETL")
        logger.error(f"Type d'erreur: {type(e).__name__}")
        logger.error(f"Message: {str(e)}", exc_info=True)
        logger.error("=" * 80)
        raise

if __name__ == "__main__":
    main()
