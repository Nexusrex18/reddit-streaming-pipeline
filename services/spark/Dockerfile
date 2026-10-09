FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
# Bake Kafka & PostgreSQL connector jars
RUN cd /opt/spark/jars && for u in \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar ; \
    do curl -fsSLO "$u"; done

# Install python NLP and database libraries
RUN pip install --no-cache-dir nltk psycopg2-binary
RUN python3 -m nltk.downloader -d /usr/local/share/nltk_data vader_lexicon && \
    python3 -c "import zipfile; zipfile.ZipFile('/usr/local/share/nltk_data/sentiment/vader_lexicon.zip').extractall('/usr/local/share/nltk_data/sentiment')" && \
    chmod -R 755 /usr/local/share/nltk_data
ENV NLTK_DATA=/usr/local/share/nltk_data

WORKDIR /app
COPY hdfs_sink.py batch_sentiment.py scheduler.py /app/
COPY classifiers /app/classifiers/
RUN chown -R spark:spark /app
USER spark
