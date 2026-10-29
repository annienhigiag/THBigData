"""
Script làm giàu dữ liệu liên tục: mô phỏng NGƯỜI DÙNG thao tác trên một nền tảng xem phim
và đẩy từng sự kiện (event) vào Kafka topic `movie-events`.

Mỗi người dùng ảo có "sở thích" (thể loại ưa thích) cố định, nên dữ liệu sinh ra có quy luật
chứ không ngẫu nhiên thuần tuý -> Spark có thể phân tích & dùng để huấn luyện lại mô hình.

Các loại sự kiện:
  view             - mở trang phim
  click_trailer    - xem trailer
  add_to_watchlist - thêm vào danh sách xem sau
  watch_complete   - xem hết phim
  rate             - chấm điểm phim (0.5 -> 5.0), dùng để làm giàu bảng ratings

Ví dụ chạy:
  # trong container airflow (Kafka nội bộ: kafka:9092)
  python /opt/airflow/scripts/user_activity_generator.py --rate 5 --duration 0
  # trên máy host (Kafka mở cổng localhost:29092)
  pip install "kafka-python>=2.0.3"
  python scripts/user_activity_generator.py --bootstrap localhost:29092
  # chỉ in ra màn hình, không gửi Kafka
  python scripts/user_activity_generator.py --dry-run --max-events 20
"""
import argparse
import csv
import json
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

EVENT_TYPES = ["view", "click_trailer", "add_to_watchlist", "watch_complete", "rate"]
EVENT_WEIGHTS = [0.45, 0.15, 0.10, 0.10, 0.20]
DEVICES = ["mobile", "web", "smart_tv", "tablet"]
DEVICE_WEIGHTS = [0.5, 0.25, 0.15, 0.10]
CITIES = ["Ha Noi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Hue", "Nha Trang", "Vung Tau"]
CITY_WEIGHTS = [0.28, 0.32, 0.1, 0.08, 0.07, 0.05, 0.05, 0.05]

_running = True


def _stop(*_):
    global _running
    _running = False


def load_catalog(data_dir):
    """Đọc movies.csv (+ ratings.csv nếu có) để biết phim nào, thể loại gì, độ phổ biến & điểm TB."""
    movies, genres_of = [], {}
    with open(os.path.join(data_dir, "movies.csv"), encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mid = int(row["movieId"])
            genres = [g for g in row["genres"].split("|") if g and g != "(no genres listed)"]
            movies.append(mid)
            genres_of[mid] = genres

    count, total = defaultdict(int), defaultdict(float)
    max_user = 610
    ratings_path = os.path.join(data_dir, "ratings.csv")
    if os.path.exists(ratings_path):
        with open(ratings_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                mid = int(row["movieId"])
                count[mid] += 1
                total[mid] += float(row["rating"])
                max_user = max(max_user, int(row["userId"]))
    popularity = [1 + count[m] for m in movies]
    avg = {m: (total[m] / count[m] if count[m] else 3.5) for m in movies}
    all_genres = sorted({g for gs in genres_of.values() for g in gs})
    return movies, genres_of, popularity, avg, all_genres, max_user


class UserSimulator:
    def __init__(self, data_dir, seed=None):
        self.rng = random.Random(seed)
        (self.movies, self.genres_of, self.popularity,
         self.avg_rating, self.all_genres, self.max_user) = load_catalog(data_dir)
        self.next_new_user = self.max_user + 1
        self.prefs = {}
        self.sessions = {}
        self.by_genre = defaultdict(list)
        for m, w in zip(self.movies, self.popularity):
            for g in self.genres_of[m]:
                self.by_genre[g].append((m, w))
        self.hot_movie, self.hot_until = None, 0.0

    def user_prefs(self, user_id):
        if user_id not in self.prefs:
            r = random.Random(user_id)  # cùng user -> cùng sở thích qua các lần chạy
            self.prefs[user_id] = r.sample(self.all_genres, k=min(3, len(self.all_genres)))
        return self.prefs[user_id]

    def pick_user(self):
        # 3% là người dùng mới đăng ký
        if self.rng.random() < 0.03:
            uid = self.next_new_user
            self.next_new_user += 1
            return uid, True
        return self.rng.randint(1, self.next_new_user - 1), False

    def pick_movie(self, user_id):
        now = time.time()
        # thỉnh thoảng có một phim "hot" (trending) trong ~2 phút
        if now > self.hot_until:
            self.hot_movie = self.rng.choices(self.movies, weights=self.popularity, k=1)[0]
            self.hot_until = now + 120
        if self.rng.random() < 0.15:
            return self.hot_movie
        if self.rng.random() < 0.7:
            genre = self.rng.choice(self.user_prefs(user_id))
            cands = self.by_genre.get(genre)
            if cands:
                ms, ws = zip(*cands)
                return self.rng.choices(ms, weights=ws, k=1)[0]
        return self.rng.choices(self.movies, weights=self.popularity, k=1)[0]

    def rating_for(self, user_id, movie_id):
        base = self.avg_rating.get(movie_id, 3.5)
        liked = set(self.genres_of.get(movie_id, [])) & set(self.user_prefs(user_id))
        score = base + (0.6 if liked else -0.3) + self.rng.gauss(0, 0.6)
        return max(0.5, min(5.0, round(score * 2) / 2))

    def next_event(self):
        user_id, is_new = self.pick_user()
        sess = self.sessions.get(user_id)
        if not sess or self.rng.random() < 0.1:
            sess = uuid.uuid4().hex[:12]
            self.sessions[user_id] = sess
        movie_id = self.pick_movie(user_id)
        event_type = self.rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        now = datetime.now(timezone.utc)
        event = {
            "event_id": uuid.uuid4().hex,
            "event_time": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "user_id": user_id,
            "session_id": sess,
            "movie_id": movie_id,
            "event_type": event_type,
            "rating": None,
            "watch_seconds": None,
            "device": self.rng.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
            "city": self.rng.choices(CITIES, weights=CITY_WEIGHTS, k=1)[0],
            "is_new_user": is_new,
        }
        if event_type == "rate":
            event["rating"] = self.rating_for(user_id, movie_id)
        elif event_type == "watch_complete":
            event["watch_seconds"] = self.rng.randint(60 * 70, 60 * 160)
        elif event_type == "view":
            event["watch_seconds"] = self.rng.randint(5, 600)
        return event


def build_producer(bootstrap):
    from kafka import KafkaProducer  # pip install "kafka-python>=2.0.3"

    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                key_serializer=lambda k: str(k).encode(),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                acks=1,
            )
        except Exception as exc:  # Kafka chưa sẵn sàng
            print(f"Chưa kết nối được Kafka ({exc}), thử lại ({attempt + 1}/30)...", flush=True)
            time.sleep(5)
    raise SystemExit("Không kết nối được Kafka")


def main():
    default_data = os.path.join(os.environ.get("PROJECT_DATA_DIR", "data"), "movielens")
    p = argparse.ArgumentParser(description="Sinh sự kiện người dùng liên tục vào Kafka")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
    p.add_argument("--topic", default="movie-events")
    p.add_argument("--data-dir", default=default_data)
    p.add_argument("--rate", type=float, default=5.0, help="số sự kiện / giây")
    p.add_argument("--duration", type=int, default=0, help="chạy bao nhiêu giây (0 = chạy mãi)")
    p.add_argument("--max-events", type=int, default=0, help="dừng sau N sự kiện (0 = không giới hạn)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--dry-run", action="store_true", help="chỉ in ra, không gửi Kafka")
    args = p.parse_args()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    sim = UserSimulator(args.data_dir, seed=args.seed)
    producer = None if args.dry_run else build_producer(args.bootstrap)
    print(f"Bắt đầu sinh dữ liệu -> topic '{args.topic}' @ {args.bootstrap} ({args.rate} event/s)", flush=True)

    start, sent, stats = time.time(), 0, defaultdict(int)
    interval = 1.0 / args.rate if args.rate > 0 else 0
    while _running:
        if args.duration and time.time() - start >= args.duration:
            break
        if args.max_events and sent >= args.max_events:
            break
        ev = sim.next_event()
        if producer:
            producer.send(args.topic, key=ev["user_id"], value=ev)
        else:
            print(json.dumps(ev, ensure_ascii=False))
        sent += 1
        stats[ev["event_type"]] += 1
        if sent % 100 == 0:
            print(f"[{datetime.now():%H:%M:%S}] đã gửi {sent} sự kiện: {dict(stats)}", flush=True)
        # dao động tốc độ một chút cho giống người dùng thật
        time.sleep(interval * random.uniform(0.5, 1.5))

    if producer:
        producer.flush()
        producer.close()
    print(f"Kết thúc. Tổng cộng {sent} sự kiện: {dict(stats)}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
