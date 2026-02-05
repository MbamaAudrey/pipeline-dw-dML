# Fraud Detection Platform

Plateforme de détection de fraude : **ETL** (source → Data Warehouse), **Data Mart** dédié au ML, et **pipeline d’entraînement** des modèles.

---

## Vue d’ensemble

```
Source (PostgreSQL)  →  ETL  →  Data Warehouse (schéma en étoile)  →  Data Mart ML  →  Modèle
     transactions         Extract / Transform / Load      fact + dimensions    transactions_ml_mart   (fraud_model.pkl)
```

- **Base source** : transactions brutes (comptes, montants, types, indicateur fraude).
- **Data Warehouse** : modélisation en étoile (`dim_account`, `dim_time`, `dim_transaction_type`, `fact_transactions`).
- **Data Mart ML** : table dédiée à l’entraînement, avec features dérivées et équilibrage fraudes / non-fraudes.

---

## Data Mart (cœur du projet)

Le **Data Mart** est la couche qui prépare les données pour le machine learning. Il est construit à partir du DW à la fin de l’ETL.

### Rôle

- Partir des faits et dimensions du DW pour produire une table **prête à l’entraînement**.
- Limiter la taille du jeu (ex. ~30 k lignes) tout en **gardant toutes les fraudes** et en échantillonnant les non-fraudes (déséquilibre de classes).
- Exposer des **features explicites** : montant, différences de soldes, type de transaction, label binaire.

### Table `transactions_ml_mart`

| Colonne | Description |
|--------|-------------|
| `amount` | Montant de la transaction |
| `origin_balance_diff` | `origin_balance_before - origin_balance_after` |
| `dest_balance_diff` | `destination_balance_after - destination_balance_before` |
| `transaction_type` | Type (CASH_IN, TRANSFER, etc.) |
| `label` | Cible binaire : `true` = fraude, `false` = normale |

### Construction (équilibrage)

- **Toutes les fraudes** du DW sont conservées.
- Les **non-fraudes** sont échantillonnées aléatoirement pour atteindre une cible d’environ 30 000 lignes au total.
- Le pipeline ML lit ensuite cette table pour l’entraînement et l’évaluation.

Le script qui construit le data mart : `ETL/data_mart/build_ml_mart.py` (appelé à la fin de `main_etl.py`).

---

## Démarrage rapide

### 1. Lancer les bases (source + DW)

```bash
cd source-db
docker compose up -d
```

(Optionnel) Appliquer les migrations Flyway si besoin :

```bash
docker compose run --rm flyway migrate
```

### 2. Charger les données source

À la racine du projet (nécessite `data/paysimdataset.csv`) :

```bash
python load_transactions.py
```

### 3. Lancer l’ETL (DW + Data Mart)

```bash
python ETL/main_etl.py
```

Cela exécute : extraction → nettoyage → chargement des dimensions et des faits → **construction du data mart** `transactions_ml_mart`.

### 4. Vérifier la base source (optionnel)

```bash
python ETL/verify_source_db.py
```

---

## Structure du projet

```
ETL/
  extract/          # Lecture depuis la base source
  transform/        # Nettoyage, standardisation, validation
  load/             # Chargement DW (dim_*, fact_transactions)
  data_mart/        # Construction de transactions_ml_mart
source-db/           # Docker (PostgreSQL source + DW), migrations Flyway
ml_pipeline/        # Entraînement et évaluation des modèles
docs/               # Schémas détaillés (ex. PIPELINE_SCHEMA.md)
```

---

## Documentation détaillée

- **Schéma du pipeline** (ETL, DW, Data Mart, ML) : `docs/PIPELINE_SCHEMA.md`
- **Migrations et évolution du schéma source** : voir les commentaires et scripts dans `source-db/` (init, migrations).
