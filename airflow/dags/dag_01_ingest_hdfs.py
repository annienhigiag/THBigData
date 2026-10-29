"""
DAG 01 - Thu thập dữ liệu: tải MovieLens -> đẩy lên HDFS (vùng raw của data lake)
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_common import DEFAULT_ARGS, HDFS_RAW_DIR, hdfs

with DAG(
    dag_id="01_ingest_movielens_to_hdfs",
    description="Tải bộ dữ liệu MovieLens và lưu vào HDFS",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["bigdata", "hdfs", "ingest"],
) as dag:
    download = BashOperator(
        task_id="download_dataset",
        bash_command="python /opt/airflow/scripts/download_dataset.py --out /opt/airflow/data/movielens",
    )

    wait_hdfs = BashOperator(
        task_id="wait_hdfs_ready",
        bash_command=hdfs("dfsadmin -safemode wait"),
    )

    make_dirs = BashOperator(
        task_id="create_hdfs_dirs",
        bash_command=hdfs(
            f"dfs -mkdir -p {HDFS_RAW_DIR} /datalake/streaming /datalake/curated "
            "/datalake/analytics /models"
        ),
    )

    put_files = BashOperator(
        task_id="put_files_to_hdfs",
        bash_command=hdfs(f"dfs -put -f /data/movielens/ratings.csv /data/movielens/movies.csv "
                          f"/data/movielens/tags.csv /data/movielens/links.csv {HDFS_RAW_DIR}/"),
    )

    verify = BashOperator(
        task_id="verify_hdfs",
        bash_command=f"{hdfs(f'dfs -ls -h {HDFS_RAW_DIR}')} && {hdfs(f'dfs -count -h {HDFS_RAW_DIR}')}",
    )

    download >> wait_hdfs >> make_dirs >> put_files >> verify
