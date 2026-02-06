import os
import sys

_MLP = os.path.dirname(os.path.abspath(__file__))
if _MLP not in sys.path:
    sys.path.insert(0, _MLP)

import joblib
from data.load_data import load_ml_mart
from validation.validate_data import validate_data
from preparation.clean_data import clean_data
from preparation.feature_engineering import build_features
from preparation.scaling import scale_data
from training.split import split_data
from training.train_model import train_model
from training.evaluate import evaluate_model

def run_pipeline():
    df = load_ml_mart()
    df = validate_data(df)
    df = clean_data(df)
    df = build_features(df)

    X_train, X_test, y_train, y_test = split_data(df)
    X_train, X_test, scaler = scale_data(X_train, X_test)

    model = train_model(X_train, y_train)
    metrics = evaluate_model(model, X_test, y_test)

    joblib.dump(model, os.path.join(_MLP, "artifacts", "models", "fraud_model.pkl"))
    joblib.dump(scaler, os.path.join(_MLP, "artifacts", "scalers", "scaler.pkl"))

    print("Pipeline terminé")
    print(metrics["roc_auc"])

if __name__ == "__main__":
    run_pipeline()
