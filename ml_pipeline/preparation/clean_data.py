def clean_data(df):
    df = df.copy()

    df.drop_duplicates(inplace=True)

    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    return df
