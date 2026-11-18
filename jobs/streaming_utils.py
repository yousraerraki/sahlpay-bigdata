# -*- coding: utf-8 -*-
"""Outils communs aux jobs de streaming (Parties 4 et 5)."""
import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from sahlpay_common import KAFKA_BOOTSTRAP, LOG_DIR, TOPIC, TX_SCHEMA


def read_kafka(spark, starting_offsets="latest", group_prefix="sahlpay", max_offsets=None):
    """Source Kafka -> DataFrame (key, value, topic, partition, offset, timestamp...)."""
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", starting_offsets)
              .option("failOnDataLoss", "false")
              .option("groupIdPrefix", group_prefix))
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets))
    return reader.load()


def parse_transactions(kafka_df):
    """Colonne binaire `value` -> colonnes typees avec from_json et le schema explicite (Partie 3)."""
    return (kafka_df
            .select(F.col("key").cast("string").alias("kafka_key"),
                    F.col("partition").alias("kafka_partition"),
                    F.col("offset").alias("kafka_offset"),
                    F.col("timestamp").alias("kafka_ts"),
                    F.from_json(F.col("value").cast("string"), TX_SCHEMA,
                                {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ssX"}).alias("tx"))
            .select("tx.*", "kafka_partition", "kafka_offset", "kafka_ts"))


def amount_band_col(col):
    return (F.when(col < 50, "micro").when(col <= 2000, "standard").otherwise("eleve"))


class ProgressLogger(StreamingQueryListener):
    """Journalise la progression de chaque micro-batch (debit, latence, watermark, lignes
    rejetees par le watermark) dans logs/<prefix>_progress.jsonl, et affiche un resume.
    Peut aussi recopier les offsets traites dans un consumer group Kafka (pour comparer avec
    kafka-consumer-groups.sh --describe)."""

    def __init__(self, prefix, commit_group=None, commit_query=None, verbose=True):
        os.makedirs(LOG_DIR, exist_ok=True)
        self.path = os.path.join(LOG_DIR, f"{prefix}_progress.jsonl")
        self.commit_group = commit_group
        self.commit_query = commit_query
        self.verbose = verbose
        self._consumer = None

    def onQueryStarted(self, event):
        print(f"[listener] requete demarree : name={event.name} id={event.id} runId={event.runId}", flush=True)

    def onQueryProgress(self, event):
        p = event.progress
        d = json.loads(p.json)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(d) + "\n")
        dropped = sum(s.get("numRowsDroppedByWatermark", 0) for s in d.get("stateOperators", []))
        state_rows = sum(s.get("numRowsTotal", 0) for s in d.get("stateOperators", []))
        wm = d.get("eventTime", {}).get("watermark", "-")
        if self.verbose and d.get("numInputRows", 0) > 0 or dropped:
            print(f"[progress] {d['name']:<22} batch={d['batchId']:<5} in={d['numInputRows']:<6} "
                  f"in/s={d.get('inputRowsPerSecond', 0):7.1f} proc/s={d.get('processedRowsPerSecond', 0):7.1f} "
                  f"duree={d.get('durationMs', {}).get('triggerExecution', 0)}ms watermark={wm} "
                  f"etat={state_rows} rejetes_watermark={dropped}", flush=True)
        if self.commit_group and d["name"] == self.commit_query:
            self._commit_offsets(d)

    def onQueryIdle(self, event):  # Spark >= 3.5
        pass

    def onQueryTerminated(self, event):
        print(f"[listener] requete terminee : id={event.id} exception={event.exception}", flush=True)

    def _commit_offsets(self, d):
        """Recopie endOffset (offsets traites et commits dans le checkpoint) dans un consumer group."""
        try:
            from kafka import KafkaConsumer, TopicPartition
            from kafka.structs import OffsetAndMetadata
            end = json.loads(d["sources"][0]["endOffset"]) if isinstance(d["sources"][0]["endOffset"], str) \
                else d["sources"][0]["endOffset"]
            if self._consumer is None:
                self._consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, group_id=self.commit_group,
                                               enable_auto_commit=False)
            offsets = {}
            for topic, parts in end.items():
                for part, off in parts.items():
                    tp = TopicPartition(topic, int(part))
                    try:
                        offsets[tp] = OffsetAndMetadata(int(off), "spark-checkpoint", -1)
                    except TypeError:
                        offsets[tp] = OffsetAndMetadata(int(off), "spark-checkpoint")
            self._consumer.assign(list(offsets))
            self._consumer.commit(offsets)
        except Exception as e:  # le monitoring ne doit jamais casser le pipeline
            print(f"[listener] commit des offsets dans le groupe {self.commit_group} impossible : {e}", flush=True)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def await_all(spark, queries, run_seconds=0):
    """Attend la fin des requetes ; Ctrl+C ou --run-seconds => arret propre (checkpoint coherent)."""
    try:
        if run_seconds:
            time.sleep(run_seconds)
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[main] interruption demandee", flush=True)
    finally:
        for q in queries:
            try:
                if q.isActive:
                    print(f"[main] arret de {q.name} (dernier batch = {q.lastProgress['batchId'] if q.lastProgress else '-'})",
                          flush=True)
                    q.stop()
            except Exception as e:
                print(f"[main] erreur a l'arret de {q.name}: {e}")
        print(f"[main] {now_iso()} toutes les requetes sont arretees", flush=True)
