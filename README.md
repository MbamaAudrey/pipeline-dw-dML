## Source DB (PostgreSQL) — migrations sans perte de données

### Pourquoi ne pas modifier `init/` ?
Les scripts copiés dans `/docker-entrypoint-initdb.d/` **ne s'exécutent qu'au tout premier démarrage**
(quand le volume Postgres est vide). Après, si tu modifies le schéma, ces scripts ne sont plus rejoués
à moins de supprimer le volume (ce qui efface les données).

### Solution recommandée : migrations SQL versionnées (Flyway)
- Les scripts vivent dans `migrations/` (ex: `V1__...sql`, `V2__...sql`, etc.)
- Flyway garde l'historique dans une table `flyway_schema_history` dans la DB
- Pour faire évoluer le schéma **sans effacer les données**, tu ajoutes **un nouveau** fichier de migration
  (tu n'édites pas une ancienne migration déjà appliquée).

### Démarrer Postgres + appliquer les migrations
Depuis le dossier `source-db/` :

```bash
docker compose up -d source-postgres
docker compose run --rm flyway migrate
```

Ou en une seule commande (Flyway va s'exécuter puis s'arrêter) :

```bash
docker compose up -d --build
```

### Ajouter une table / colonne plus tard
Exemples :

- Nouvelle migration `V2__add_users_table.sql` :
  - `CREATE TABLE ...`
- Nouvelle migration `V3__add_transactions_status.sql` :
  - `ALTER TABLE transactions ADD COLUMN status text;`

Puis :

```bash
docker compose run --rm flyway migrate
```

### Important (anti-perte de données)
- **Ne fais pas** `docker compose down -v` en production/dev si tu veux garder les données (le `-v` supprime le volume).
- Fais des changements incrémentaux (`ALTER TABLE`, nouvelles tables, nouveaux index).

