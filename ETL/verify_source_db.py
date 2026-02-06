#!/usr/bin/env python3
"""
Vérification de la base de données source (connexion, table transactions, nombre de lignes).
À lancer depuis la racine du projet : python ETL/verify_source_db.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ETL.db import get_connection
from ETL.config import SRC_DB


def main():
    print("🔍 Vérification de la base source")
    print(f"   {SRC_DB['host']}:{SRC_DB['port']}/{SRC_DB['dbname']}")
    print()

    try:
        conn = get_connection(SRC_DB)
        cur = conn.cursor()

        # Table existe ?
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'transactions'
            );
        """)
        exists = cur.fetchone()[0]
        if not exists:
            print("❌ La table 'transactions' n'existe pas dans la base source.")
            print("   → Lancer les migrations (Flyway) ou créer la table avec source-db/init/01_create_tables.sql")
            conn.close()
            return 1

        print("✅ Table 'transactions' présente")

        # Nombre de lignes
        cur.execute("SELECT COUNT(*) FROM transactions")
        count = cur.fetchone()[0]
        print(f"   Lignes dans transactions: {count:,}")

        if count == 0:
            print()
            print("⚠️  La table est vide. L'ETL extraira 0 lignes.")
            print("   → Charger les données avec: python load_transactions.py")
            print("   → (nécessite data/paysimdataset.csv)")
        else:
            # Colonnes
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'transactions'
                ORDER BY ordinal_position;
            """)
            cols = [r[0] for r in cur.fetchall()]
            print(f"   Colonnes: {', '.join(cols)}")
            # Un échantillon
            cur.execute("SELECT step, transaction_type, origin_account, amount FROM transactions LIMIT 1")
            row = cur.fetchone()
            if row:
                print(f"   Exemple: step={row[0]}, type={row[1]}, origin={row[2]}, amount={row[3]}")

        conn.close()
        print()
        print("✅ Vérification terminée.")
        return 0

    except Exception as e:
        print(f"❌ Erreur: {e}")
        print("   → Vérifier que le conteneur source tourne: docker compose -f source-db/docker-compose.yml ps")
        print("   → Port 5433 exposé pour source-postgres")
        return 1


if __name__ == "__main__":
    sys.exit(main())
