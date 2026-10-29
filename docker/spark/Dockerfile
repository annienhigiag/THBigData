# Spark 3.5.1 + Python + connector Kafka (đóng sẵn jar để không phải tải lúc chạy)
FROM apache/spark:3.5.1-python3

USER root
RUN pip3 install --no-cache-dir numpy==1.24.4 \
 && cd /opt/spark/jars \
 && for j in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar ; do \
      curl -fsSLO "https://repo1.maven.org/maven2/$j" ; done \
 && mkdir -p /opt/spark-apps /opt/data /opt/output /tmp/spark-events \
 && chown -R spark:spark /opt/spark-apps /opt/output /tmp/spark-events

ENV PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    HADOOP_USER_NAME=root

USER spark
WORKDIR /opt/spark-apps
