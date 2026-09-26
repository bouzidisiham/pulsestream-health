from pyspark.sql import SparkSession
import logging

import os
import sys

"""Configuration et création de la SparkSession.
"""

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

def get_spark_session(app_name: str = "PulseStream") -> SparkSession:
    """Crée ou récupère une SparkSession configurée pour le dev local.

    Args:
        app_name: Nom de l'application (visible dans les logs et l'UI).

    Returns:
        Une SparkSession prête à l'emploi.
    """

    logging.getLogger("py4j").setLevel(logging.WARNING)

    builder = SparkSession.builder \
    .appName(app_name) \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.sql.shuffle.partitions","4") \
    .config("spark.driver.memory","2g") \
    .config("spark.sql.adaptive.enabled","true")

    return builder.getOrCreate()


