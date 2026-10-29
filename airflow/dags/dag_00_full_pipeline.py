"""
DAG 00 - Chạy toàn bộ pipeline theo đúng thứ tự (dùng để demo):
  01 ingest HDFS  ->  02 streaming (Kafka + Spark Streaming)  ->  03 batch analytics + ML
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pipeline_common import DEFAULT_ARGS

with DAG(
    dag_id="00_full_pipeline",
    description="Điều phối toàn bộ: HDFS -> Kafka/Spark Streaming -> Spark batch & ML",
    default_args={**DEFAULT_ARGS, "retries": 0},
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["bigdata", "orchestration"],
) as dag:
    common = dict(wait_for_completion=True, poke_interval=15, reset_dag_run=True)

    ingest = TriggerDagRunOperator(task_id="run_01_ingest", trigger_dag_id="01_ingest_movielens_to_hdfs", **common)
    stream = TriggerDagRunOperator(task_id="run_02_streaming", trigger_dag_id="02_streaming_user_events",
                                   conf={"duration_seconds": 180, "events_per_second": 10}, **common)
    batch = TriggerDagRunOperator(task_id="run_03_batch_ml", trigger_dag_id="03_batch_analytics_and_ml", **common)

    ingest >> stream >> batch
