"""
DAG 02 - Luồng thời gian thực:
  script sinh dữ liệu người dùng  ->  Kafka (movie-events)  ->  Spark Structured Streaming  ->  HDFS

Hai task chạy SONG SONG trong cùng khoảng thời gian:
  - spark_streaming_job : Spark đọc Kafka, làm giàu, ghi HDFS
  - generate_user_events: script mô phỏng người dùng bắn sự kiện vào Kafka
Bấm Trigger (có thể truyền tham số duration_seconds / events_per_second).
"""
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from pipeline_common import DEFAULT_ARGS, KAFKA_TOPIC, spark_submit

with DAG(
    dag_id="02_streaming_user_events",
    description="Sinh sự kiện người dùng -> Kafka -> Spark Streaming -> HDFS",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 10, 1),
    schedule=None,  # chạy khi được DAG 00 gọi hoặc bấm Trigger (có thể đổi thành "@hourly")
    catchup=False,
    max_active_runs=1,
    params={
        "duration_seconds": Param(300, type="integer", minimum=30, description="Thời gian sinh dữ liệu (giây)"),
        "events_per_second": Param(5, type="number", minimum=0.1, description="Tốc độ sinh sự kiện"),
    },
    tags=["bigdata", "kafka", "spark-streaming"],
) as dag:
    create_topic = BashOperator(
        task_id="create_kafka_topic",
        bash_command=(
            "docker exec bd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 "
            f"--create --if-not-exists --topic {KAFKA_TOPIC} --partitions 3 --replication-factor 1 && "
            "docker exec bd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 "
            f"--describe --topic {KAFKA_TOPIC}"
        ),
    )

    streaming = BashOperator(
        task_id="spark_streaming_job",
        # chạy lâu hơn generator 90s để xử lý hết dữ liệu & chốt các cửa sổ thời gian
        bash_command=spark_submit("streaming_events.py",
                                  "--timeout {{ params.duration_seconds + 90 }}", cores=1),
    )

    generate = BashOperator(
        task_id="generate_user_events",
        # đợi Spark Streaming khởi động xong rồi mới bắt đầu bắn sự kiện
        bash_command=(
            "sleep 30 && python /opt/airflow/scripts/user_activity_generator.py "
            "--bootstrap kafka:9092 --topic " + KAFKA_TOPIC + " "
            "--data-dir /opt/airflow/data/movielens "
            "--duration {{ params.duration_seconds }} --rate {{ params.events_per_second }}"
        ),
    )

    check_hdfs = BashOperator(
        task_id="check_streaming_output",
        bash_command=(
            "docker exec bd-namenode hdfs dfs -du -s -h /datalake/streaming/events /datalake/streaming/trending"
        ),
    )

    create_topic >> [streaming, generate]
    [streaming, generate] >> check_hdfs
