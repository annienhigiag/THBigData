"""
Spark MLlib: huấn luyện mô hình gợi ý phim (Collaborative Filtering - ALS)

  Input : /datalake/curated/ratings_all  (ratings gốc + ratings mới từ luồng streaming)
          (nếu chưa có thì dùng /datalake/raw/movielens/ratings.csv)
  Output: /models/als/latest                     mô hình đã huấn luyện (HDFS)
          /datalake/analytics/recommendations    top-10 phim gợi ý cho mỗi người dùng
          /opt/output/model/metrics.json         RMSE / MAE, tham số tốt nhất
          /opt/output/model/sample_recommendations.csv, all_recommendations.csv (dùng cho trang web)

Chạy: spark-submit --master spark://spark-master:7077 /opt/spark-apps/train_model.py
"""
import csv
import json
import os
from datetime import datetime

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

HDFS = "hdfs://namenode:9000"
LOCAL_OUT = "/opt/output/model"


def main():
    spark = (SparkSession.builder.appName("train_als_recommender")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(f"{HDFS}/tmp/als_checkpoint")

    try:
        ratings = spark.read.parquet(f"{HDFS}/datalake/curated/ratings_all")
    except AnalysisException:
        ratings = (spark.read.option("header", True).option("inferSchema", True)
                   .csv(f"{HDFS}/datalake/raw/movielens/ratings.csv")
                   .withColumn("source", F.lit("historical")))
    ratings = ratings.select(F.col("userId").cast("int"), F.col("movieId").cast("int"),
                             F.col("rating").cast("float"), "source").cache()
    movies = (spark.read.option("header", True).option("inferSchema", True)
              .csv(f"{HDFS}/datalake/raw/movielens/movies.csv"))

    counts = {r["source"]: r["count"] for r in ratings.groupBy("source").count().collect()}
    print(f"Số ratings dùng để huấn luyện: {counts}")

    train, test = ratings.randomSplit([0.8, 0.2], seed=42)
    rmse_eval = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    mae_eval = RegressionEvaluator(metricName="mae", labelCol="rating", predictionCol="prediction")

    # Dò tham số đơn giản (grid search nhỏ) -> chọn mô hình có RMSE thấp nhất
    results, best = [], None
    for rank in (10, 20):
        for reg in (0.05, 0.1):
            als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", rank=rank,
                      regParam=reg, maxIter=10, coldStartStrategy="drop", nonnegative=True, seed=42)
            model = als.fit(train)
            pred = model.transform(test)
            rmse, mae = rmse_eval.evaluate(pred), mae_eval.evaluate(pred)
            results.append({"rank": rank, "regParam": reg, "rmse": round(rmse, 4), "mae": round(mae, 4)})
            print(f"rank={rank:<3} regParam={reg:<5} RMSE={rmse:.4f}  MAE={mae:.4f}")
            if best is None or rmse < best[0]:
                best = (rmse, mae, rank, reg, model)

    rmse, mae, rank, reg, model = best
    print(f"\n==> Mô hình tốt nhất: rank={rank}, regParam={reg}, RMSE={rmse:.4f}, MAE={mae:.4f}")

    # Huấn luyện lại trên toàn bộ dữ liệu với tham số tốt nhất rồi lưu
    final = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", rank=rank, regParam=reg,
                maxIter=10, coldStartStrategy="drop", nonnegative=True, seed=42).fit(ratings)
    version = datetime.now().strftime("%Y%m%d_%H%M%S")
    final.write().overwrite().save(f"{HDFS}/models/als/{version}")
    final.write().overwrite().save(f"{HDFS}/models/als/latest")

    # Top-10 gợi ý cho từng người dùng
    recs = (final.recommendForAllUsers(10)
            .select("userId", F.posexplode("recommendations").alias("rank", "rec"))
            .select("userId", (F.col("rank") + 1).alias("rank"),
                    F.col("rec.movieId").alias("movieId"),
                    F.round("rec.rating", 3).alias("predicted_rating"))
            .join(movies.select("movieId", "title", "genres"), "movieId")
            .orderBy("userId", "rank"))
    recs.write.mode("overwrite").parquet(f"{HDFS}/datalake/analytics/recommendations")

    os.makedirs(LOCAL_OUT, exist_ok=True)
    sample_users = [1, 2, 3, 100, 414]
    all_recs = recs.orderBy("userId", "rank").collect()  # ~6.000 dòng, đủ nhỏ để xuất CSV cho web
    header = ["userId", "rank", "movieId", "title", "genres", "predicted_rating"]
    for name, rows in (("sample_recommendations.csv", [r for r in all_recs if r["userId"] in sample_users]),
                       ("all_recommendations.csv", all_recs)):
        with open(os.path.join(LOCAL_OUT, name), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows([[r[c] for c in header] for r in rows])
    recs.filter(F.col("userId") == 1).show(10, truncate=50)

    metrics = {
        "trained_at": version,
        "algorithm": "ALS (Alternating Least Squares) - Spark MLlib",
        "num_ratings": counts,
        "train_test_split": "80/20, seed=42",
        "grid_search": results,
        "best": {"rank": rank, "regParam": reg, "rmse": round(rmse, 4), "mae": round(mae, 4)},
        "model_path": f"{HDFS}/models/als/{version}",
    }
    with open(os.path.join(LOCAL_OUT, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    # lịch sử các lần huấn luyện (để so sánh khi dữ liệu được làm giàu dần)
    hist_path = os.path.join(LOCAL_OUT, "training_history.csv")
    new_file = not os.path.exists(hist_path)
    with open(hist_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["trained_at", "historical_ratings", "streaming_ratings", "rank", "regParam", "rmse", "mae"])
        w.writerow([version, counts.get("historical", 0), counts.get("streaming", 0), rank, reg,
                    round(rmse, 4), round(mae, 4)])
    print(json.dumps(metrics, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
