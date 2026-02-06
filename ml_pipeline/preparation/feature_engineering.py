import pandas as pd


def build_features(df):
    df = df.copy()

    if "transaction_type" in df.columns:
        df = pd.get_dummies(
            df,
            columns=["transaction_type"],
            drop_first=True,
        )

    return df
