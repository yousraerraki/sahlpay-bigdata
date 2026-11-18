# SahlPay.ma : plateforme Big Data pour un portefeuille mobile

Ce dépôt est le projet « Écosystème Big Data 2 » (5ème année EMSI, 2026-2027). Il contient une chaîne analytique complète qui va de Kafka aux tableaux de bord :

```
producer.py → Kafka (topic transactions, 3 partitions) → Spark Structured Streaming → MinIO
                                                         (Bronze → Silver → Gold / Delta Lake)
                                                       → GraphFrames (PySpark) + GraphX (Scala)
```

## 1. Versions utilisées

| Composant | Version | Remarque |
|---|---|---|
| Apache Spark | **3.5.3** (standalone : 1 Master et 2 Workers) | image `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` |
| Scala | **2.12.18** | version embarquée par Spark 3.5.3, utilisée pour GraphX |
| Java | 17 | |
| Python | 3.10 (dans l'image) | même version pour le driver (Jupyter) et les executors |
| Apache Kafka | **3.8.0**, mode **KRaft** (sans ZooKeeper) | image `apache/kafka:3.8.0` |
| Connecteur Kafka | `spark-sql-kafka-0-10_2.12:3.5.3`, `kafka-clients:3.4.1` | |
| Delta Lake | **3.2.1** (`delta-spark_2.12`, `delta-storage`) | Delta 3.2.x correspond à Spark 3.5.x |
| GraphFrames | **0.8.4-spark3.5-s_2.12** (repli automatique sur 0.8.3) | package Spark Packages |
| MinIO (S3) | `RELEASE.2025-04-22T22-12-26Z`, accès via `hadoop-aws:3.3.4` et `aws-java-sdk-bundle:1.12.262` | |
| Python (libs) | jupyterlab 4.2, kafka-python ≥ 2.0.3, pandas, pyarrow, networkx, matplotlib | |

Tous les jars sont téléchargés pendant `docker compose build` et placés dans `/opt/spark/jars`. Ils sont donc disponibles pour le driver comme pour les executors, sans `--packages`. Le module Python `graphframes` est extrait du jar et installé dans le `site-packages` de l'image (voir `docker/spark/Dockerfile`).

> Si l'image MinIO indiquée n'est plus disponible, vous pouvez changer d'image sans toucher au fichier compose : `MINIO_IMAGE=minio/minio:latest docker compose up -d`. Le même mécanisme existe pour Kafka (`KAFKA_IMAGE`) et pour le client MinIO (`MC_IMAGE`).

## 2. Arborescence

```
docker-compose.yml          Kafka (KRaft), kafka-init, MinIO, minio-init, Spark master + 2 workers, Jupyter
docker/spark/               Dockerfile (jars Kafka/Delta/S3A/GraphFrames), spark-defaults.conf, log4j2
producer/producer.py        générateur (modes merchants | file | kafka) : retards, doublons, fraude injectée
data/merchants.json         référentiel statique (300 lignes JSON)
data/fraud_ground_truth.json  vérité terrain des scénarios injectés (générée par producer.py)
notebooks/                  01 cluster | 02 RDD | 03 Spark SQL | 06 GraphFrames + GraphX
jobs/                       sahlpay_common.py, streaming_utils.py, part4_streaming.py,
                            part5_pipeline.py, part5_verify.py
graphx/                     src/main/scala/SahlPayGraphX.scala, run_graphx.sh, build.sbt
scripts/                    00_init … 07_captures (une commande par étape)
logs/                       temps mesurés, progression des micro-batches, comptages avant/après
rapport/captures/           captures et preuves pour le rapport
```

Organisation du bucket `lake` (MinIO) :

```
lake/
  bronze/history/                 transactions_history.jsonl (Partie 2)
  bronze/referentiel/             merchants.json
  bronze/transactions_raw/        JSON bruts lus dans Kafka (Partie 4)
  silver/transactions_history/    Parquet nettoyé, partitionné par city (Partie 3)
  silver/transactions_clean/      Parquet nettoyé + enrichi, partitionné par city et date (Partie 5)
  silver/graph/                   sommets et arêtes du graphe (Partie 6)
  gold/kpi_city_10min/            Delta Lake : KPI fenêtre tumbling 10 min (Partie 5)
  gold/reject_rate_city_sliding/  Delta Lake : taux de rejet (fenêtre glissante 10 min / 5 min)
  gold/device_sessions/           Delta Lake : sessions par appareil (session_window 5 min)
  gold/graph_analysis/            anomalies, partitionnées par anomaly_type (Partie 6)
  gold/graphx_metrics/            degrés, composantes, PageRank (GraphX)
  gold/graph_dashboard/           métriques pour le tableau de bord conformité
  checkpoints/                    checkpoints Structured Streaming (un par requête)
```

## 3. Lancement pas à pas

Prérequis : Docker Desktop (ou Docker Engine) avec Compose v2, au moins **8 Go de RAM** alloués à Docker et 4 cœurs.

### Partie 1 : cluster et positionnement
```bash
./scripts/00_init.sh          # build + up + vérif topic (3 partitions), bucket lake, workers
```
- Spark Master UI : http://localhost:8080 (on doit y voir **2 Workers ALIVE**)
- MinIO console : http://localhost:9001 (identifiants `minioadmin` / `minioadmin123`, on doit y voir le bucket **lake**)
- Jupyter : http://localhost:8888, puis exécuter `notebooks/01_cluster_positionnement.ipynb`
- Spark UI de l'application en cours : http://localhost:4040 (puis 4041, etc. si plusieurs applications tournent)

### Partie 2 : SparkCore (RDD)
```bash
./scripts/01_generate_and_upload.sh      # producer.py --mode file --n 50000 + copie vers lake/bronze/history/
```
Exécutez ensuite `notebooks/02_sparkcore_rdd.ipynb`. Pendant l'exécution, faites la capture du DAG dans l'onglet Jobs de http://localhost:4040.

### Partie 3 : Spark SQL et DataFrames
Exécutez `notebooks/03_sparksql_dataframes.ipynb`. Les sorties de `explain(True)` sont aussi enregistrées dans `logs/explain_join_*.txt`, et les temps mesurés dans `logs/timings_part*.json`.

### Partie 4 : Structured Streaming
Ouvrez deux terminaux :
```bash
./scripts/02_start_producer.sh                     # terminal 1 : 20 ev/s, avec un événement en retard de 15 min (et un de 5 min) toutes les 60 s
./scripts/03_part4_streaming.sh raw                # terminal 2 : console append + Bronze JSON
./scripts/03_part4_streaming.sh counts             #   ou : ville x statut, outputMode complete
./scripts/03_part4_streaming.sh watermark          #   ou : watermark 10 min + sonde des retards
```
La progression de chaque micro-batch (débit, durée, watermark, `numRowsDroppedByWatermark`) est enregistrée dans `logs/part4_progress.jsonl`. Pour retrouver l'événement `TX-LATE15M-…` (rejeté par le watermark) et l'événement `TX-LATE05M-…` (accepté), comparez `logs/producer.log`, la sortie de la sonde `p4_late_probe` et la ligne `rejetes_watermark=1` du listener.

### Partie 5 : pipeline de production
Arrêtez d'abord la Partie 4 (Ctrl+C), mais laissez le producteur tourner.
```bash
./scripts/04_part5_pipeline.sh                   # Silver + 3 tables Gold (Ctrl+C = arrêt propre)
./scripts/05_part5_restart_demo.sh               # démonstration complète de reprise (environ 10 min)
docker compose exec jupyter spark-submit jobs/part5_verify.py --label controle   # Delta + DESCRIBE HISTORY
```
Le script `05` enchaîne les étapes suivantes : exécution du pipeline, comptage, affichage des offsets du checkpoint et de `kafka-consumer-groups.sh --describe`, arrêt de 2 minutes, redémarrage, puis nouveau comptage. Tout est écrit dans `logs/part5_restart_demo.log` et `logs/part5_counts.csv`.

Le groupe `sahlpay-pipeline-monitor` reçoit, après chaque micro-batch, les offsets enregistrés dans le checkpoint. Cela permet de les comparer avec `kafka-consumer-groups.sh`. Spark, lui, ne commite jamais ses offsets dans Kafka : sa source de vérité est le checkpoint.

### Partie 6 : GraphFrames et GraphX
Exécutez `notebooks/06_graphes_graphframes_graphx.ipynb`. Le notebook :
1. construit les sommets et les arêtes depuis la couche Silver, avec le contrôle des arêtes orphelines ;
2. lance GraphFrames : motifs (appareils partagés, cycles, chaînes) et degrés ;
3. lance automatiquement **GraphX en Scala** (`graphx/run_graphx.sh`, via `spark-shell -i`) ;
4. compare les degrés obtenus par les deux outils, applique les seuils, vérifie les résultats contre la vérité terrain et estime les faux positifs ;
5. écrit les résultats dans `lake/gold/graph_analysis/` (partitionné par `anomaly_type`) et produit la visualisation `rapport/captures/part6_sous_graphe_suspect.png`.

Pour lancer GraphX seul : `./scripts/06_part6_graphx.sh`. Pour l'exécuter en jar compilé plutôt qu'en script : `cd graphx && sbt package`, puis `spark-submit --class SahlPayGraphX target/scala-2.12/sahlpay-graphx_2.12-1.0.jar`.

Par défaut, la Partie 6 utilise **les deux couches Silver** (historique et flux). Pour changer ce comportement : `GRAPH_SOURCE=history` ou `GRAPH_SOURCE=stream` dans l'environnement du conteneur jupyter.

### Captures obligatoires (checklist)
- [ ] Spark Master UI avec les 2 Workers actifs (port 8080)
- [ ] DAG et stages dans la Spark UI (Parties 2 et 3)
- [ ] Sortie de `explain(True)` (Partie 3, aussi dans `logs/explain_join_broadcast.txt`)
- [ ] Sink console sur plusieurs micro-batches (Partie 4)
- [ ] `mc ls --recursive local/lake/`, obtenu avec `./scripts/07_captures_lake.sh`
- [ ] `DESCRIBE HISTORY` sur la table Delta Gold (`part5_verify.py`)
- [ ] `kafka-consumer-groups.sh --describe` (`logs/part5_restart_demo.log`)
- [ ] Partie 6 : modèle du graphe, motif GraphFrames, résultat GraphX, liste des cas suspects, comparaison des degrés, exports Parquet

## 4. Partage des ressources

Le cluster dispose de 4 cœurs (2 Workers × 2 cœurs, 2 Go chacun). Les notebooks 01 à 03 prennent les 4 cœurs : pensez à exécuter leur dernière cellule `spark.stop()`. Les jobs streaming et GraphX sont limités à 2 cœurs (`spark.cores.max=2`), ce qui permet de faire tourner deux applications en même temps (pipeline et vérification, ou notebook 06 et GraphX). Si une application reste en état `WAITING` dans l'UI du Master, une autre application occupe déjà les cœurs.

## 5. Dépannage

| Symptôme | Solution |
|---|---|
| `Initial job has not accepted any resources` | Une autre application occupe les cœurs. Arrêtez-la (`spark.stop()` ou Ctrl+C). |
| `Python in worker has different version` | Lancez toujours le driver depuis le conteneur `jupyter`, qui utilise la même image que les workers. |
| Les executors n'arrivent pas à joindre le driver | Vérifiez que `SPARK_LOCAL_HOSTNAME=jupyter` est bien défini (déjà fait dans le fichier compose). |
| Rien n'apparaît dans les tables Gold | La fenêtre de 10 min doit avoir reçu des données ; la table Delta est créée au premier micro-batch non vide. |
| Repartir de zéro | `docker compose down -v`, puis `./scripts/00_init.sh` |

Projet réalisé en binôme : Yousra Erraki – Rania Bennani.
