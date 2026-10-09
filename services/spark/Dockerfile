FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
# Bake the Kafka connector jars in so startup never needs the internet
RUN cd /opt/spark/jars && for u in \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar ; \
    do curl -fsSLO "$u"; done
WORKDIR /app
COPY hdfs_sink.py /app/
RUN chown -R spark:spark /app
USER spark
