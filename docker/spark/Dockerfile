# Image unique utilisee par le Spark Master, les deux Workers et le notebook Jupyter.
# Le driver (Jupyter) et les executors (Workers) ont ainsi exactement les memes versions
# de Spark, Scala, Java et Python, ce qui evite les erreurs "Python in worker has different version".
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ARG SPARK_PACKAGES=https://repos.spark-packages.org
ARG SPARK_VERSION=3.5.3
ARG DELTA_VERSION=3.2.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG GRAPHFRAMES_VERSION=0.8.4-spark3.5-s_2.12

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl unzip procps \
 && rm -rf /var/lib/apt/lists/*

# ---- Jars ajoutes a /opt/spark/jars (charges automatiquement par le driver et les executors) ----
#  * Kafka          : spark-sql-kafka + kafka-clients + token-provider + commons-pool2
#  * MinIO (S3A)    : hadoop-aws + aws-java-sdk-bundle (versions alignees sur Hadoop 3.3.4 de Spark 3.5.3)
#  * Delta Lake     : delta-spark + delta-storage (Delta 3.2.x <-> Spark 3.5.x)
#  * GraphFrames    : graphframes 0.8.4 pour Spark 3.5 / Scala 2.12
WORKDIR /opt/spark/jars
RUN set -eux; \
    for u in \
      ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
      ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
      ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
      ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      ${MAVEN}/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
      ${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    ; do curl -fsSLO --retry 3 "$u"; done; \
    curl -fsSL --retry 3 -o graphframes.jar \
      ${SPARK_PACKAGES}/graphframes/graphframes/${GRAPHFRAMES_VERSION}/graphframes-${GRAPHFRAMES_VERSION}.jar \
    || curl -fsSL --retry 3 -o graphframes.jar \
      ${SPARK_PACKAGES}/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar; \
    ls -la /opt/spark/jars | grep -E "kafka|aws|delta|graphframes|pool2"

# ---- Python : PySpark de l'image + librairies du projet ----
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH
RUN pip3 install --no-cache-dir \
      jupyterlab==4.2.5 ipykernel \
      "kafka-python>=2.0.3" \
      pandas==2.2.3 pyarrow==17.0.0 \
      networkx==3.3 matplotlib==3.9.2 \
 && pip3 install --no-cache-dir --no-deps delta-spark==${DELTA_VERSION} \
 && SITE=$(python3 -c "import site; print(site.getsitepackages()[0])") \
 && cd "$SITE" && unzip -q -o /opt/spark/jars/graphframes.jar 'graphframes/*' \
 && python3 -c "import graphframes, delta, kafka; print('python libs OK')"

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j2.properties /opt/spark/conf/log4j2.properties

WORKDIR /home/sahlpay
