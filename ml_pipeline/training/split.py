from sklearn.model_selection import train_test_split

from config.settings import RANDOM_STATE, TARGET, TEST_SIZE


def split_data(df):
   
    df = df.sample(frac=1, random_state=RANDOM_STATE).reset_index(drop=True)
    X = df.drop(columns=[TARGET])
    y = df[TARGET]
    return train_test_split(
        X, y,
        test_size=TEST_SIZE,
        stratify=y,
        shuffle=True,
        random_state=RANDOM_STATE,
    )
