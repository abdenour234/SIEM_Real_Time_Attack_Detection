# -------------------------------
# Dockerfile Spark Streaming + Kafka
# -------------------------------
FROM apache/spark:3.4.1-python3

# Utilisateur root pour installer les paquets
USER root

# Installer pip et curl
RUN apt-get update && apt-get install -y python3-pip curl && apt-get clean

# Définir le répertoire de travail
WORKDIR /app

# -------------------------------
# Ajouter les jars Spark-Kafka nécessaires
# -------------------------------
RUN mkdir -p $SPARK_HOME/jars

# Spark Kafka connector (OBLIGATOIRE)
RUN curl -L -o $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar

# Kafka client libs (compatible Confluent 7.x)
RUN curl -L -o $SPARK_HOME/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# Avro support
RUN curl -L -o $SPARK_HOME/jars/spark-avro_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.4.1/spark-avro_2.12-3.4.1.jar

# Avoid connection pool crash (optional)
RUN curl -L -o $SPARK_HOME/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# -------------------------------
# Installer les dépendances Python
# -------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script Spark consumer
COPY spark_consumer.py .

# Répertoire pour les checkpoints Spark Streaming
RUN mkdir -p /tmp/spark-checkpoints

# -------------------------------
# Commande par défaut pour lancer le consumer
# -------------------------------
CMD ["spark-submit", "--master", "local[*]", "/app/spark_consumer.py"]
