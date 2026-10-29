"""Các hằng số / hàm dùng chung cho các DAG của đồ án."""
from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "bigdata",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

HDFS_RAW_DIR = "/datalake/raw/movielens"
KAFKA_TOPIC = "movie-events"


def spark_submit(script, args="", cores=2):
    """Lệnh spark-submit chạy job PySpark trên Spark cluster (driver nằm trong container spark-master)."""
    return (
        "docker exec -u root bd-spark-master spark-submit "
        "--master spark://spark-master:7077 "
        f"--executor-memory 900m --total-executor-cores {cores} "
        f"/opt/spark-apps/{script} {args}"
    )


def hdfs(cmd):
    """Chạy lệnh `hdfs dfs ...` bên trong container namenode."""
    return f"docker exec bd-namenode hdfs {cmd}"
