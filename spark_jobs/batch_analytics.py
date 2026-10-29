"""
Spark batch: truy vấn dữ liệu trên HDFS -> bảng phân tích (đầu ra dữ liệu)

  Input (HDFS):
    /datalake/raw/movielens/*.csv            dữ liệu gốc MovieLens
    /datalake/streaming/events               sự kiện người dùng do Spark Streaming ghi (nếu có)
  Output:
    /datalake/curated/ratings_all            ratings gốc + ratings mới từ streaming (dữ liệu đã làm giàu)
    /datalake/analytics/<bảng>               các bảng phân tích (Parquet)
    /opt/output/analytics/<bảng>.csv         bản CSV để vẽ biểu đồ / đưa vào báo cáo

Chạy: spark-submit --master spark://spark-master:7077 /opt/spark-apps/batch_analytics.py
"""
import csv
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

HDFS = "hdfs://namenode:9000"
RAW = f"{HDFS}/datalake/raw/movielens"
LOCAL_OUT = "/opt/output/analytics"


def save(df, name, show=10):
    """Ghi bảng kết quả lên HDFS (parquet) + xuất CSV ra thư mục output để làm báo cáo."""
    df.write.mode("overwrite").parquet(f"{HDFS}/datalake/analytics/{name}")
    rows = df.collect()
    os.makedirs(LOCAL_OUT, exist_ok=True)
    with open(os.path.join(LOCAL_OUT, f"{name}.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        w.writerows([list(r) for r in rows])
    print(f"\n### {name} ({len(rows)} dòng) -> HDFS /datalake/analytics/{name}")
    df.show(show, truncate=45)


def main():
    spark = (SparkSession.builder.appName("batch_movielens_analytics")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    ratings = spark.read.option("header", True).option("inferSchema", True).csv(f"{RAW}/ratings.csv")
    movies = spark.read.option("header", True).option("inferSchema", True).csv(f"{RAW}/movies.csv")
    tags = spark.read.option("header", True).option("inferSchema", True).csv(f"{RAW}/tags.csv")

    movies = (movies
              .withColumn("release_year", F.regexp_extract("title", r"\((\d{4})\)\s*$", 1).cast("int"))
              .withColumn("genre_list", F.split("genres", r"\|")))

    # ------------------------------------------------------------------
    # 1. Làm giàu: gộp ratings lịch sử với ratings mới sinh ra từ luồng Kafka/Streaming
    # ------------------------------------------------------------------
    hist = ratings.select("userId", "movieId", "rating",
                          F.from_unixtime("timestamp").cast("timestamp").alias("rated_at"),
                          F.lit("historical").alias("source"))
    try:
        events = spark.read.parquet(f"{HDFS}/datalake/streaming/events")
        has_stream = True
    except AnalysisException:
        events, has_stream = None, False
        print("Chưa có dữ liệu streaming trên HDFS -> chỉ phân tích dữ liệu gốc.")

    if has_stream:
        stream_ratings = (events.filter(F.col("event_type") == "rate")
                          .select(F.col("user_id").alias("userId"), F.col("movie_id").alias("movieId"),
                                  "rating", F.col("event_ts").alias("rated_at"),
                                  F.lit("streaming").alias("source")))
        ratings_all = hist.unionByName(stream_ratings)
    else:
        ratings_all = hist
    ratings_all.write.mode("overwrite").parquet(f"{HDFS}/datalake/curated/ratings_all")
    ratings_all = spark.read.parquet(f"{HDFS}/datalake/curated/ratings_all")

    ratings_all.createOrReplaceTempView("ratings")
    movies.createOrReplaceTempView("movies")
    tags.createOrReplaceTempView("tags")

    save(spark.sql("""
        SELECT source, COUNT(*) AS num_ratings, COUNT(DISTINCT userId) AS num_users,
               COUNT(DISTINCT movieId) AS num_movies, ROUND(AVG(rating), 3) AS avg_rating
        FROM ratings GROUP BY source ORDER BY source
    """), "dataset_overview")

    # ------------------------------------------------------------------
    # 2. Các truy vấn phân tích bằng Spark SQL
    # ------------------------------------------------------------------
    save(spark.sql("""
        SELECT m.movieId, m.title, m.genres, COUNT(*) AS num_ratings,
               ROUND(AVG(r.rating), 3) AS avg_rating
        FROM ratings r JOIN movies m ON r.movieId = m.movieId
        GROUP BY m.movieId, m.title, m.genres
        HAVING COUNT(*) >= 50
        ORDER BY avg_rating DESC, num_ratings DESC
        LIMIT 20
    """), "top_rated_movies")

    save(spark.sql("""
        SELECT m.movieId, m.title, COUNT(*) AS num_ratings, ROUND(AVG(r.rating), 3) AS avg_rating
        FROM ratings r JOIN movies m ON r.movieId = m.movieId
        GROUP BY m.movieId, m.title
        ORDER BY num_ratings DESC LIMIT 20
    """), "most_popular_movies")

    save(spark.sql("""
        SELECT genre, COUNT(DISTINCT m.movieId) AS num_movies, COUNT(*) AS num_ratings,
               ROUND(AVG(r.rating), 3) AS avg_rating
        FROM ratings r
        JOIN (SELECT movieId, explode(genre_list) AS genre FROM movies) m ON r.movieId = m.movieId
        WHERE genre <> '(no genres listed)'
        GROUP BY genre ORDER BY num_ratings DESC
    """), "genre_stats", show=20)

    save(spark.sql("""
        SELECT CAST(FLOOR(release_year / 10) * 10 AS INT) AS decade,
               COUNT(DISTINCT m.movieId) AS num_movies, COUNT(*) AS num_ratings,
               ROUND(AVG(r.rating), 3) AS avg_rating
        FROM ratings r JOIN movies m ON r.movieId = m.movieId
        WHERE release_year IS NOT NULL
        GROUP BY 1 ORDER BY decade
    """), "ratings_by_decade", show=20)

    save(spark.sql("""
        SELECT YEAR(rated_at) AS year, COUNT(*) AS num_ratings,
               COUNT(DISTINCT userId) AS active_users, ROUND(AVG(rating), 3) AS avg_rating
        FROM ratings GROUP BY 1 ORDER BY year
    """), "ratings_per_year", show=40)

    save(spark.sql("""
        SELECT rating, COUNT(*) AS cnt,
               ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM ratings GROUP BY rating ORDER BY rating
    """), "rating_distribution", show=20)

    save(spark.sql("""
        SELECT userId, COUNT(*) AS num_ratings, ROUND(AVG(rating), 3) AS avg_rating,
               MIN(rated_at) AS first_rating, MAX(rated_at) AS last_rating
        FROM ratings GROUP BY userId ORDER BY num_ratings DESC LIMIT 20
    """), "top_active_users")

    save(spark.sql("""
        SELECT LOWER(tag) AS tag, COUNT(*) AS cnt, COUNT(DISTINCT movieId) AS num_movies
        FROM tags GROUP BY LOWER(tag) ORDER BY cnt DESC LIMIT 30
    """), "top_tags")

    # ------------------------------------------------------------------
    # 3. Phân tích hành vi người dùng từ dữ liệu streaming
    # ------------------------------------------------------------------
    if has_stream:
        events.createOrReplaceTempView("events")
        save(spark.sql("""
            SELECT event_type, COUNT(*) AS cnt, COUNT(DISTINCT user_id) AS users
            FROM events GROUP BY event_type ORDER BY cnt DESC
        """), "stream_event_types")

        save(spark.sql("""
            SELECT city, device, COUNT(*) AS events, COUNT(DISTINCT user_id) AS users,
                   ROUND(AVG(rating), 2) AS avg_rating
            FROM events GROUP BY city, device ORDER BY events DESC
        """), "stream_city_device", show=20)

        save(spark.sql("""
            SELECT primary_genre AS genre,
                   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
                   SUM(CASE WHEN event_type = 'click_trailer' THEN 1 ELSE 0 END) AS trailer_clicks,
                   SUM(CASE WHEN event_type = 'add_to_watchlist' THEN 1 ELSE 0 END) AS watchlist_adds,
                   SUM(CASE WHEN event_type = 'watch_complete' THEN 1 ELSE 0 END) AS completes,
                   SUM(CASE WHEN event_type = 'rate' THEN 1 ELSE 0 END) AS ratings,
                   ROUND(SUM(CASE WHEN event_type = 'watch_complete' THEN 1 ELSE 0 END)
                         / NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 3)
                         AS view_to_complete_rate
            FROM events WHERE primary_genre IS NOT NULL
            GROUP BY primary_genre ORDER BY views DESC
        """), "stream_genre_funnel", show=20)

        save(spark.sql("""
            SELECT date_trunc('minute', event_ts) AS minute, COUNT(*) AS events,
                   COUNT(DISTINCT user_id) AS active_users
            FROM events GROUP BY 1 ORDER BY 1
        """), "stream_events_per_minute", show=20)

        try:
            trending = spark.read.parquet(f"{HDFS}/datalake/streaming/trending")
            save(trending.groupBy("movie_id", "title", "primary_genre")
                 .agg(F.sum("events").alias("events"), F.sum("views").alias("views"),
                      F.max("events").alias("peak_events_per_minute"))
                 .orderBy(F.desc("events")).limit(20), "stream_trending_movies")
        except AnalysisException:
            pass

    spark.stop()


if __name__ == "__main__":
    main()
