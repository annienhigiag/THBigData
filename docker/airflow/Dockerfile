FROM apache/airflow:2.9.3-python3.11

# docker CLI: để task Airflow gọi `docker exec` vào spark-master / namenode / kafka
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
