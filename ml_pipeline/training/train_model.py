from sklearn.ensemble import RandomForestClassifier

from config.settings import RANDOM_STATE


def train_model(X_train, y_train):
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=12,
        class_weight="balanced",
        random_state=RANDOM_STATE,
    )
    model.fit(X_train, y_train)
    return model
