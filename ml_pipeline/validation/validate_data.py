import logging

logger = logging.getLogger(__name__)

def validate_data(df):
    if df.empty:
        raise ValueError("Dataset vide")

    valid_labels = {0, 1, True, False}
    invalid_mask = ~df["label"].isin(valid_labels)

    if invalid_mask.any():
        invalid_values = df.loc[invalid_mask, "label"].unique()
        logger.error(
            f"Valeurs invalides dans label: {invalid_values}"
        )
        raise ValueError("Le label doit être binaire (0/1 ou True/False)")

    fraud_ratio = df["label"].mean()
    logger.info(f"Taux de fraude: {fraud_ratio:.4f}")

    return df
