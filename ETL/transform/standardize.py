import logging

logger = logging.getLogger(__name__)

def standardize_columns(df):
    columns_before = df.columns.tolist()
    logger.debug(f"Colonnes avant standardisation: {len(columns_before)} colonnes")
    
    rename_map = {
        "type": "transaction_type",
        "nameOrig": "origin_account",
        "nameDest": "destination_account",
        "oldbalanceOrg": "origin_balance_before",
        "newbalanceOrig": "origin_balance_after",
        "oldbalanceDest": "destination_balance_before",
        "newbalanceDest": "destination_balance_after",
        "isFraud": "is_fraud",
        "isFlaggedFraud": "is_flagged_fraud"
    }
    
    df = df.rename(columns=rename_map)
    columns_after = df.columns.tolist()
    
    renamed_count = len([c for c in rename_map.keys() if c in columns_before])
    logger.info(f"  → Colonnes renommées: {renamed_count} colonnes")
    logger.debug(f"Colonnes après standardisation: {', '.join(columns_after)}")
    
    return df
