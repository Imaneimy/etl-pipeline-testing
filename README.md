# etl-pipeline-testing

Ce projet vient d'un problème concret que j'avais en stage chez Orange Maroc — les données achats arrivaient de SAP en CSV, et on savait jamais vraiment si le format allait être propre. On vérifiait à la main dans Excel avant de les charger dans Power BI. J'ai voulu formaliser ça avec un vrai pipeline de tests automatisés.

Le pipeline est en PySpark avec des tests Pytest. Les cas de test suivent le format XRAY/JIRA qu'on utilisait en équipe pour tracker les anomalies.

## Structure

```
src/etl/          — extract, transform, load, pipeline
src/validators/   — schema validator, business rules
tests/unit/       — tests unitaires transform + validators
tests/integration — test end-to-end du pipeline complet
data/raw/         — sales.csv avec quelques anomalies intentionnelles (et une pas intentionnelle)
docs/             — cas de test format XRAY
draft/            — mes notes perso de dev (brouillon)
```

## Lancer les tests 🛠️

```bash
pip install -r requirements.txt
pytest tests/ -v
```

Pour juste les tests rapides :
```bash
pytest tests/unit/ -v
```

## Ce qui m'a bloquée

### SparkSession qui polluait les tests
Au début j'avais la fixture avec `scope="function"` — Spark se recréait à chaque test mais pas proprement, et parfois les données d'un test apparaissaient dans le suivant. Les tests passaient ou échouaient aléatoirement selon l'ordre.

Solution : `scope="session"` + appel explicite à `session.stop()` à la fin. Voir conftest.py.

### Le port 4040 déjà occupé
Si je relançais les tests trop vite, j'avais "address already in use" sur le port 4040 (Spark UI). Réglé avec `spark.ui.enabled=false`.

### simpleString() entre Spark 3.3 et 3.4
Dans `validate_schema`, j'utilise `simpleString()` pour comparer les types. En Spark 3.3 vs 3.4, certains types (LongType) changent de représentation. À surveiller si on passe à Spark 3.5.

### handle_nulls avec strategy="mean" sur colonne string
Ça ne plante pas mais ça ne fait rien — limitation connue, documentée dans draft/notes.md.

## Ce que je ferais différemment

- Tester avec de vrais fichiers Parquet/Delta au lieu de DataFrames créés à la main
- Ajouter une CI — j'ai essayé avec GitHub Actions mais PySpark est lourd à setup sur un runner standard
- Mieux gérer les types dans validate_schema pour que ce soit stable entre versions Spark

## Stack

PySpark 3.4, Pytest, Python

