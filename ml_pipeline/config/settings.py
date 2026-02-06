RANDOM_STATE = 42
TEST_SIZE = 0.2
TARGET = "label"

NUMERIC_FEATURES = [
    "amount",
    "origin_balance_diff",
    "dest_balance_diff"
]

CATEGORICAL_FEATURES = [
    "transaction_type"
]

MODEL_PATH = "ml_pipeline/models/model.pkl"

LOG_FILE = "ml_pipeline/logs/ml_pipeline.log"

LOG_LEVEL = "INFO"

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"