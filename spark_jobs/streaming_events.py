"""
Spark Structured Streaming: Kafka (movie-events) -> làm giàu -> HDFS

Luồng xử lý:
  1. Đọc liên tục các sự kiện JSON từ Kafka topic `movie-events`.
  2. Parse JSON theo schema, làm giàu bằng cách JOIN với bảng phim (movies.csv trên HDFS)
     để có thêm tên phim, năm phát hành, thể loại.
  3. Sink 1: ghi toàn bộ sự kiện đã làm giàu xuống HDFS (Parquet, phân vùng theo ngày)
             -> /datalake/streaming/events
  4. Sink 2: thống kê theo cửa sổ thời gian 1 phút (phim trending, điểm TB) có watermark
             -> /datalake/streaming/trending  + in top 10 ra console mỗi micro-batch

Chạy (trong container spark-master):
  spark-submit --master spark://spark-master:7077 /opt/spark-apps/streaming_events.py --timeout 600
"""
import argparse

from pyspark.sql import SparkSession, functions as F, types as T

HDFS = "hdfs://namenode:9000"

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("user_id", T.IntegerType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("movie_id", T.IntegerType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("rating", T.DoubleType()),
    T.StructField("watch_seconds", T.IntegerType()),
    T.StructField("device", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("is_new_user", T.BooleanType()),
])


def load_movies(spark):
    movies = (spark.read.option("header", True).option("inferSchema", True)
              .csv(f"{HDFS}/datalake/raw/movielens/movies.csv"))
    return (movies
            .withColumn("release_year", F.regexp_extract("title", r"\((\d{4})\)\s*$", 1).cast("int"))
            .withColumn("primary_genre", F.split("genres", r"\|").getItem(0))
            .select(F.col("movieId").alias("movie_id"), "title", "genres", "primary_genre", "release_year"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--topic", default="movie-events")
    p.add_argument("--timeout", type=int, default=0, help="số giây chạy rồi dừng (0 = chạy mãi)")
    p.add_argument("--starting-offsets", default="earliest",
                   help="chỉ có tác dụng lần chạy đầu; các lần sau đọc tiếp từ checkpoint")
    args = p.parse_args()

    spark = (SparkSession.builder.appName("streaming_movie_events")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.port", "4041")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    movies = F.broadcast(load_movies(spark))

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", args.starting_offsets)
           .option("failOnDataLoss", "false")
           .load())

    events = (raw.select(F.col("value").cast("string").alias("json"),
                         F.col("timestamp").alias("kafka_ts"))
              .select(F.from_json("json", EVENT_SCHEMA).alias("e"), "kafka_ts")
              .select("e.*", "kafka_ts")
              .withColumn("event_ts", F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
              .withColumn("event_date", F.to_date("event_ts"))
              .withColumn("event_hour", F.hour("event_ts"))
              .filter(F.col("event_id").isNotNull()))

    # --- Làm giàu: stream-static join với danh mục phim ---
    enriched = events.join(movies, on="movie_id", how="left")

    ckpt = f"{HDFS}/datalake/streaming/_checkpoints"

    q_events = (enriched.drop("event_time").writeStream
                .queryName("events_to_hdfs")
                .format("parquet")
                .option("path", f"{HDFS}/datalake/streaming/events")
                .option("checkpointLocation", f"{ckpt}/events")
                .partitionBy("event_date")
                .outputMode("append")
                .trigger(processingTime="20 seconds")
                .start())

    trending = (enriched
                .withWatermark("event_ts", "30 seconds")
                .groupBy(F.window("event_ts", "1 minute"), "movie_id", "title", "primary_genre")
                .agg(F.count("*").alias("events"),
                     F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
                     F.sum(F.when(F.col("event_type") == "rate", 1).otherwise(0)).alias("ratings"),
                     F.round(F.avg("rating"), 2).alias("avg_rating"),
                     F.approx_count_distinct("user_id").alias("unique_users"))
                .select(F.col("window.start").alias("window_start"),
                        F.col("window.end").alias("window_end"),
                        "movie_id", "title", "primary_genre", "events", "views",
                        "ratings", "avg_rating", "unique_users"))

    def write_trending(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        batch_df.write.mode("append").parquet(f"{HDFS}/datalake/streaming/trending")
        print(f"\n===== Batch {batch_id}: Top 10 phim trending (cửa sổ 1 phút đã chốt) =====")
        (batch_df.orderBy(F.desc("events")).limit(10)
         .select("window_start", "title", "primary_genre", "events", "views", "avg_rating", "unique_users")
         .show(truncate=40))

    q_trend = (trending.writeStream
               .queryName("trending_1min")
               .outputMode("append")
               .foreachBatch(write_trending)
               .option("checkpointLocation", f"{ckpt}/trending")
               .trigger(processingTime="20 seconds")
               .start())

    # Console: số sự kiện theo loại trong mỗi micro-batch (để demo)
    q_console = (events.groupBy("event_type").count().writeStream
                 .queryName("event_type_counts")
                 .outputMode("complete")
                 .format("console")
                 .trigger(processingTime="20 seconds")
                 .start())

    if args.timeout > 0:
        spark.streams.awaitAnyTermination(args.timeout)
        for q in (q_console, q_trend, q_events):
            q.stop()
        print(f"Đã chạy {args.timeout}s, dừng streaming.")
    else:
        spark.streams.awaitAnyTermination()
    spark.stop()


if __name__ == "__main__":
    main()
