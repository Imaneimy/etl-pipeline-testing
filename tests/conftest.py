"""
fixtures partagées pour tous les tests du projet.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # scope="session" important — j'avais mis "function" au début et les tests
    # se polluaient entre eux, des données d'un test apparaissaient dans le suivant
    # spark.ui.enabled=false sinon le port 4040 reste occupé si on relance les tests
    # trop vite → "address already in use"
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("ETL_Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
