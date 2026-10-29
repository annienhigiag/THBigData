"""
DAG 03 - Xử lý lô (batch):
  HDFS (raw + dữ liệu streaming đã làm giàu) -> Spark SQL phân tích -> huấn luyện mô hình ALS
Đầu ra: bảng phân tích trên HDFS + CSV trong thư mục output/, mô hình trong HDFS /models/als
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_common import DEFAULT_ARGS, HDFS_RAW_DIR, hdfs, spark_submit

with DAG(
    dag_id="03_batch_analytics_and_ml",
    description="Spark truy vấn dữ liệu HDFS -> phân tích + huấn luyện mô hình gợi ý",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 10, 1),
    schedule=None,  # có thể đổi thành "@daily" để chạy định kỳ
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "spark", "ml"],
) as dag:
    check_input = BashOperator(
        task_id="check_raw_data_on_hdfs",
        bash_command=hdfs(f"dfs -test -e {HDFS_RAW_DIR}/ratings.csv") + " && " + hdfs(f"dfs -ls {HDFS_RAW_DIR}"),
    )

    analytics = BashOperator(
        task_id="spark_batch_analytics",
        bash_command=spark_submit("batch_analytics.py"),
    )

    train = BashOperator(
        task_id="train_als_model",
        bash_command=spark_submit("train_model.py"),
    )

    report = BashOperator(
        task_id="show_results",
        bash_command=(
            "docker exec bd-namenode hdfs dfs -ls /datalake/analytics /models/als && "
            "docker exec bd-spark-master cat /opt/output/model/metrics.json"
        ),
    )

    check_input >> analytics >> train >> report
