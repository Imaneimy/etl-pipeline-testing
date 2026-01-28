# notes perso — projet ETL testing

## pourquoi ce projet
j'ai eu des problèmes similaires en stage chez Orange Maroc — les données achats
venaient de SAP en CSV et on savait jamais vraiment si le format allait être propre.
en prod on vérifiait à la main dans Excel. j'ai voulu formaliser ça en tests.

## problèmes rencontrés pendant le dev

### SparkSession qui se ferme pas
pendant longtemps mes tests s'influençaient mutuellement — des données d'un test
apparaissaient dans le suivant. j'ai mis du temps à comprendre que c'était le scope
de la fixture qui était "function" par défaut, donc Spark se recréait à chaque test
mais pas proprement.

solution : scope="session" + session.stop() explicite à la fin
→ voir conftest.py

### port 4040 déjà occupé
si je relançais les tests trop vite, j'avais "port 4040 already in use"
→ spark.ui.enabled=false dans la config, ça règle le problème

### simpleString() entre Spark 3.3 et 3.4
validate_schema utilisait simpleString() pour comparer les types.
ça marchait en 3.3 mais en 3.4 le format de string pour certains types a changé
(LongType → "bigint" vs "long"). j'ai dû hardcoder dans les tests.
à surveiller si on passe à Spark 3.5.

### handle_nulls avec strategy="mean" sur une colonne string
ça plante silencieusement — Spark calcule pas la moyenne d'une string, retourne null,
et fillna avec null ne fait rien. à corriger mais pour l'instant c'est dans le README
en tant que limitation connue.

## questions à creuser
- peut-on tester Delta Lake sans Databricks ? (pyspark-delta en local ?)
- comment mocker un JDBC proprement en test sans vraie DB ?
- pytest-spark : j'ai regardé mais trop de config pour pas grand chose

## refs utiles
- https://spark.apache.org/docs/latest/api/python/
- pytest docs sur les fixtures et les scopes
- pas utilisé pytest-spark finalement
