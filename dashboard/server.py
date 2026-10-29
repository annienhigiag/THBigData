"""
Web dashboard (FastAPI) cho đồ án Big Data - http://localhost:8550

Backend đọc:
  - Kafka topic movie-events  : sự kiện người dùng mới nhất (realtime)
  - output/analytics/*.csv    : kết quả Spark SQL batch
  - output/model/*            : kết quả huấn luyện ALS + gợi ý phim
  - data/movielens/*.csv      : danh mục phim, lịch sử đánh giá
và cung cấp API JSON cho giao diện web trong thư mục static/.
"""
import glob
import json
import os
import socket
import threading
from datetime import datetime, timedelta, timezone

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

OUTPUT = os.environ.get("OUTPUT_DIR", "/app/output")
DATA = os.environ.get("DATA_DIR", "/app/data/movielens")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "movie-events")
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

SERVICES = [
    # (tên, host, cổng nội bộ, link mở từ máy host)
    ("HDFS NameNode", "namenode", 9870, "http://localhost:9870"),
    ("HDFS DataNode", "datanode", 9864, "http://localhost:9864"),
    ("Kafka Broker", "kafka", 9092, None),
    ("Kafka UI", "kafka-ui", 8080, "http://localhost:8085"),
    ("Spark Master", "spark-master", 8080, "http://localhost:8080"),
    ("Spark Worker", "spark-worker", 8081, "http://localhost:8081"),
    ("Airflow", "airflow", 8080, "http://localhost:8088"),
]

app = FastAPI(title="CineData Big Data Dashboard")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


# ----------------------------------------------------------------------------- helpers
def _mtime_cache(func):
    """Cache theo thời điểm sửa file: file đổi (pipeline chạy lại) thì đọc lại."""
    cache = {}

    def wrapper(path):
        try:
            m = os.path.getmtime(path)
        except OSError:
            return None
        if path not in cache or cache[path][0] != m:
            cache[path] = (m, func(path))
        return cache[path][1]
    return wrapper


@_mtime_cache
def read_csv(path):
    return pd.read_csv(path)


def records(df):
    return json.loads(df.to_json(orient="records", date_format="iso")) if df is not None else None


def movies_df():
    df = read_csv(os.path.join(DATA, "movies.csv"))
    if df is None:
        return None
    df = df.copy()
    df["year"] = df["title"].str.extract(r"\((\d{4})\)\s*$")[0]
    df["clean_title"] = df["title"].str.replace(r"\s*\(\d{4}\)\s*$", "", regex=True)
    return df


@_mtime_cache
def movie_stats(path):
    r = pd.read_csv(path)
    return r.groupby("movieId").agg(num_ratings=("rating", "size"), avg_rating=("rating", "mean"))


# ----------------------------------------------------------------------------- Kafka
_kafka_lock = threading.Lock()
_consumer = None


def latest_events(n_per_partition=500):
    global _consumer
    from kafka import KafkaConsumer, TopicPartition
    with _kafka_lock:
        if _consumer is None:
            _consumer = KafkaConsumer(bootstrap_servers=KAFKA, enable_auto_commit=False,
                                      request_timeout_ms=8000, api_version_auto_timeout_ms=5000,
                                      value_deserializer=lambda v: json.loads(v.decode("utf-8")))
        c = _consumer
        parts = c.partitions_for_topic(TOPIC)
        if not parts:
            return []
        tps = [TopicPartition(TOPIC, p) for p in parts]
        c.assign(tps)
        end, begin = c.end_offsets(tps), c.beginning_offsets(tps)
        for tp in tps:
            c.seek(tp, max(begin[tp], end[tp] - n_per_partition))
        events = []
        for _ in range(20):
            for recs in c.poll(timeout_ms=300).values():
                events.extend(r.value for r in recs)
            if all(c.position(tp) >= end[tp] for tp in tps):
                break
        return events


# ----------------------------------------------------------------------------- routes
@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


@app.get("/api/status")
def status():
    services = []
    for name, host, port, link in SERVICES:
        try:
            with socket.create_connection((host, port), timeout=0.8):
                ok = True
        except OSError:
            ok = False
        services.append({"name": name, "host": f"{host}:{port}", "online": ok, "link": link})
    metrics_path = os.path.join(OUTPUT, "model", "metrics.json")
    trained = json.load(open(metrics_path, encoding="utf-8"))["trained_at"] if os.path.exists(metrics_path) else None
    return {
        "services": services,
        "dataset_ready": os.path.exists(os.path.join(DATA, "ratings.csv")),
        "analytics_tables": sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(OUTPUT, "analytics", "*.csv"))),
        "model_trained_at": trained,
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/realtime")
def realtime():
    try:
        events = latest_events()
    except Exception as exc:  # Kafka chưa sẵn sàng
        global _consumer
        _consumer = None
        return {"ok": False, "error": str(exc)}
    if not events:
        return {"ok": True, "empty": True}

    df = pd.DataFrame(events)
    df["ts"] = pd.to_datetime(df["event_time"], utc=True)
    mv = movies_df()
    if mv is not None:
        df = df.merge(mv[["movieId", "clean_title", "year", "genres"]], how="left",
                      left_on="movie_id", right_on="movieId")
    else:
        df["clean_title"], df["year"], df["genres"] = df["movie_id"].astype(str), None, ""
    df["clean_title"] = df["clean_title"].fillna("Movie #" + df["movie_id"].astype(str))

    now = datetime.now(timezone.utc)
    w5 = df[df.ts >= now - timedelta(minutes=5)]
    live = not w5.empty
    win = w5 if live else df
    last_min = df[df.ts >= now - timedelta(minutes=1)]

    trending = (win.groupby(["movie_id", "clean_title", "year", "genres"], dropna=False)
                .agg(events=("event_id", "size"), views=("event_type", lambda s: int((s == "view").sum())),
                     avg_rating=("rating", "mean"), users=("user_id", "nunique"))
                .reset_index().sort_values("events", ascending=False).head(10))
    timeline = (df.set_index("ts").resample("10s").size().tail(36))
    feed = df.sort_values("ts", ascending=False).head(25)

    return {
        "ok": True, "live": live,
        "kpi": {
            "events_last_min": int(len(last_min)),
            "active_users_5m": int(w5.user_id.nunique()),
            "ratings_5m": int((w5.event_type == "rate").sum()),
            "avg_rating_5m": round(float(w5.rating.mean()), 2) if w5.rating.notna().any() else None,
            "new_users_5m": int(w5.is_new_user.sum()) if "is_new_user" in w5 else 0,
            "last_event": df.ts.max().isoformat(),
        },
        "trending": records(trending.rename(columns={"clean_title": "title"})),
        "event_types": win.event_type.value_counts().to_dict(),
        "cities": win.city.value_counts().to_dict(),
        "devices": win.device.value_counts().to_dict(),
        "timeline": {"labels": [t.strftime("%H:%M:%S") for t in timeline.index], "values": timeline.tolist()},
        "feed": records(feed[["event_time", "user_id", "clean_title", "event_type", "rating", "device", "city"]]
                        .rename(columns={"clean_title": "title"})),
    }


@app.get("/api/analytics")
def analytics():
    out = {}
    for p in glob.glob(os.path.join(OUTPUT, "analytics", "*.csv")):
        out[os.path.basename(p)[:-4]] = records(read_csv(p))
    return out


@app.get("/api/model")
def model():
    path = os.path.join(OUTPUT, "model", "metrics.json")
    if not os.path.exists(path):
        return {"ready": False}
    return {"ready": True, "metrics": json.load(open(path, encoding="utf-8")),
            "history": records(read_csv(os.path.join(OUTPUT, "model", "training_history.csv")))}


def _recs():
    for name in ("all_recommendations.csv", "sample_recommendations.csv"):
        df = read_csv(os.path.join(OUTPUT, "model", name))
        if df is not None:
            return df
    return None


@app.get("/api/users")
def users():
    df = _recs()
    return [] if df is None else sorted(int(u) for u in df.userId.unique())


@app.get("/api/recommendations/{user_id}")
def recommendations(user_id: int):
    df = _recs()
    if df is None:
        raise HTTPException(404, "Chưa có kết quả mô hình")
    recs = df[df.userId == user_id].sort_values("rank")
    history = []
    ratings = read_csv(os.path.join(DATA, "ratings.csv"))
    mv = movies_df()
    if ratings is not None and mv is not None:
        h = (ratings[ratings.userId == user_id].sort_values(["rating", "timestamp"], ascending=False).head(8)
             .merge(mv[["movieId", "clean_title", "year", "genres"]], on="movieId"))
        history = records(h[["movieId", "clean_title", "year", "genres", "rating"]].rename(columns={"clean_title": "title"}))
        n_rated = int((ratings.userId == user_id).sum())
    else:
        n_rated = None
    return {"user_id": user_id, "num_rated": n_rated, "history": history, "recommendations": records(recs)}


@app.get("/api/movies/search")
def search(q: str = "", limit: int = 24):
    mv = movies_df()
    if mv is None:
        return []
    res = mv[mv.title.str.contains(q, case=False, na=False, regex=False)] if q else mv
    stats_path = os.path.join(DATA, "ratings.csv")
    if os.path.exists(stats_path):
        res = res.join(movie_stats(stats_path), on="movieId")
        res = res.sort_values("num_ratings", ascending=False, na_position="last")
    res = res.head(limit)
    cols = [c for c in ("movieId", "clean_title", "year", "genres", "num_ratings", "avg_rating") if c in res]
    return records(res[cols].rename(columns={"clean_title": "title"}))
