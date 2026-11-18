# -*- coding: utf-8 -*-
"""Elements partages par tous les jobs SahlPay : SparkSession, chemins du lac et schemas explicites."""
import os
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType, TimestampType)

# Racine du lac de donnees. Dans Docker : s3a://lake (MinIO). En local : file:///tmp/lake
LAKE = os.environ.get("LAKE", "s3a://lake").rstrip("/")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
PROJECT_DIR = os.environ.get("PROJECT_DIR", os.path.normpath(os.path.join(os.path.dirname(__file__), "..")))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
LOG_DIR = os.path.join(PROJECT_DIR, "logs")


class Paths:
    bronze_history = f"{LAKE}/bronze/history"
    bronze_raw = f"{LAKE}/bronze/transactions_raw"
    silver_history = f"{LAKE}/silver/transactions_history"
    silver_clean = f"{LAKE}/silver/transactions_clean"
    silver_graph = f"{LAKE}/silver/graph"
    gold_kpi = f"{LAKE}/gold/kpi_city_10min"
    gold_reject = f"{LAKE}/gold/reject_rate_city_sliding"
    gold_sessions = f"{LAKE}/gold/device_sessions"
    gold_graph = f"{LAKE}/gold/graph_analysis"
    gold_graphx = f"{LAKE}/gold/graphx_metrics"
    gold_graph_dashboard = f"{LAKE}/gold/graph_dashboard"
    checkpoints = f"{LAKE}/checkpoints"


# Schema explicite d'un evenement de transaction (aucune inference)
TX_SCHEMA = StructType([
    StructField("tx_id", StringType(), False),
    StructField("type", StringType(), True),           # P2M | P2P
    StructField("payer_wallet", StringType(), True),
    StructField("payee_id", StringType(), True),       # M-... (P2M) ou W-... (P2P)
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("channel", StringType(), True),        # APP | QR | USSD
    StructField("status", StringType(), True),         # INITIATED | AUTHORIZED | COMPLETED | REJECTED | REVERSED
    StructField("amount_mad", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
])

# Schema explicite du referentiel merchants.json
MERCHANT_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("category", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])


def get_spark(app_name, master=None, extra_conf=None, cores_max=None):
    """SparkSession connectee au Master standalone (ou local[*] pour les tests).

    Le cluster offre 4 coeurs (2 Workers x 2). Par defaut une application prend au plus 2 coeurs
    (spark.cores.max=2, executors de 1 coeur repartis sur les deux Workers) : on peut ainsi faire
    tourner deux applications en meme temps (pipeline streaming + verification, notebook + GraphX).
    Les notebooks batch des Parties 1 a 3 demandent les 4 coeurs (cores_max=4)."""
    builder = (SparkSession.builder
               .appName(app_name)
               .master(master or SPARK_MASTER))
    if (master or SPARK_MASTER).startswith("spark://"):
        builder = (builder
                   .config("spark.driver.host", os.environ.get("SPARK_LOCAL_HOSTNAME", "jupyter"))
                   .config("spark.driver.bindAddress", "0.0.0.0")
                   .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
                   .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "1"))
                   .config("spark.cores.max", str(cores_max or os.environ.get("SPARK_CORES_MAX", "2"))))
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def timer(label, results=None):
    """Mesure du temps d'execution (wall clock) d'un bloc."""
    t0 = time.perf_counter()
    yield
    dt = time.perf_counter() - t0
    print(f"[timer] {label}: {dt:.2f} s")
    if results is not None:
        results[label] = round(dt, 3)


def merchants_path():
    """Chemin du referentiel : copie dans le lac si elle existe, sinon fichier local."""
    return os.environ.get("MERCHANTS_PATH", f"{LAKE}/bronze/referentiel/merchants.json")
