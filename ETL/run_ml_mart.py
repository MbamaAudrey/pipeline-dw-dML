#!/usr/bin/env python3
"""
Lance uniquement la construction du data mart ML (transactions_ml_mart).
À exécuter depuis la racine du projet : python ETL/run_ml_mart.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from ETL.data_mart.build_ml_mart import build_ml_mart

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

if __name__ == "__main__":
    build_ml_mart()
