# BÁO CÁO ĐỒ ÁN THỰC HÀNH BIG DATA

## Xây dựng hệ thống thu thập, xử lý luồng và phân tích dữ liệu đánh giá phim với HDFS, Kafka, Spark và Airflow

> Sinh viên: ……………… MSSV: ………… Lớp: …………
> Giảng viên hướng dẫn: ………………
>
> *Ghi chú: những chỗ đánh dấu* **[ẢNH]** *là nơi chèn ảnh chụp màn hình; những chỗ* **[SỐ LIỆU]** *lấy từ thư mục `output/` sau khi chạy.*

---

## Chương 1. Giới thiệu

### 1.1. Bối cảnh và bài toán
Các nền tảng xem phim trực tuyến (Netflix, FPT Play, VieON…) ghi nhận hàng triệu hành vi người dùng mỗi ngày: mở trang phim,
xem trailer, thêm vào danh sách xem sau, xem hết phim, chấm điểm. Dữ liệu này vừa có **khối lượng lớn**
(dữ liệu lịch sử), vừa **liên tục phát sinh** (dữ liệu luồng). Doanh nghiệp cần:

1. Lưu trữ tin cậy, mở rộng được toàn bộ dữ liệu lịch sử;
2. Bắt và xử lý sự kiện **gần thời gian thực** (phim nào đang "hot" trong phút vừa qua);
3. Phân tích tổng hợp (thể loại nào được ưa chuộng, xu hướng theo thời gian…);
4. Huấn luyện **mô hình gợi ý phim** và cập nhật mô hình khi có dữ liệu mới.

### 1.2. Mục tiêu đồ án
Xây dựng một pipeline dữ liệu lớn hoàn chỉnh theo thiết kế:

- **Data → HDFS → Spark (query) → đầu ra** (phân tích, huấn luyện mô hình);
- **Script sinh dữ liệu liên tục** (mô phỏng người dùng thao tác) → **Kafka** bắt sự kiện → **Spark Streaming** xử lý;
- **Airflow** điều phối các luồng trên.

### 1.3. Phạm vi
Toàn bộ hệ thống chạy trên một máy bằng Docker Compose (cụm giả lập phân tán gồm 8 container).

---

## Chương 2. Cơ sở lý thuyết và công nghệ

| Công nghệ | Phiên bản | Vai trò trong đồ án |
|---|---|---|
| Hadoop HDFS | 3.2.1 | Hệ thống tệp phân tán, làm *data lake* lưu dữ liệu gốc, dữ liệu luồng, kết quả và mô hình |
| Apache Kafka | 3.7.0 (KRaft) | Message broker, hứng sự kiện người dùng theo thời gian thực (topic `movie-events`, 3 partition) |
| Apache Spark | 3.5.1 | Spark SQL (truy vấn batch), Structured Streaming (xử lý luồng), MLlib (ALS) |
| Apache Airflow | 2.9.3 | Điều phối luồng công việc bằng DAG, lập lịch, theo dõi, retry |
| Docker Compose | — | Đóng gói và triển khai toàn bộ cụm |

### 2.1. HDFS
*(Trình bày kiến trúc NameNode/DataNode, block, replication; trong đồ án dùng 1 NameNode + 1 DataNode, replication = 1.)*

### 2.2. Kafka
*(Producer, Broker, Topic, Partition, Offset, Consumer; chế độ KRaft không cần Zookeeper. Sự kiện dùng `user_id` làm key
nên mọi sự kiện của một người dùng nằm cùng partition, giữ được thứ tự.)*

### 2.3. Spark
- **Spark SQL / DataFrame**: truy vấn dữ liệu trên HDFS bằng SQL.
- **Structured Streaming**: mô hình micro-batch, *watermark* xử lý dữ liệu đến trễ, *window* theo thời gian sự kiện,
  *checkpoint* đảm bảo xử lý chính xác một lần (exactly-once) khi ghi file.
- **MLlib – ALS (Alternating Least Squares)**: lọc cộng tác dựa trên phân rã ma trận user × item:
  R ≈ U·Vᵀ, tối ưu luân phiên U và V với chính quy hoá λ (`regParam`).

### 2.4. Airflow
*(DAG, Task, Operator, Scheduler, Executor; dùng LocalExecutor + PostgreSQL để các task chạy song song.)*

---

## Chương 3. Dữ liệu

### 3.1. Bộ dữ liệu MovieLens (ml-latest-small)
Nguồn: GroupLens Research, https://grouplens.org/datasets/movielens/

| File | Số dòng (≈) | Các cột |
|---|---|---|
| ratings.csv | 100.836 | userId, movieId, rating (0.5–5.0), timestamp |
| movies.csv | 9.742 | movieId, title (kèm năm), genres (phân tách bởi `|`) |
| tags.csv | 3.683 | userId, movieId, tag, timestamp |
| links.csv | 9.742 | movieId, imdbId, tmdbId |

### 3.2. Dữ liệu sinh liên tục (dữ liệu luồng)
Script `scripts/user_activity_generator.py` mô phỏng người dùng:

- Mỗi người dùng có **3 thể loại yêu thích cố định** → hành vi có quy luật, không ngẫu nhiên thuần tuý.
- Chọn phim theo độ phổ biến (số lượt đánh giá trong ratings.csv) và theo sở thích; định kỳ có một phim
  "hot" trong khoảng 2 phút (để kiểm chứng phát hiện trending).
- Điểm chấm = điểm trung bình của phim + thưởng/phạt theo sở thích + nhiễu Gauss.
- 3% sự kiện đến từ người dùng mới (userId > 610).

Cấu trúc một sự kiện (JSON):

```json
{"event_id": "117bf961...", "event_time": "2026-10-25T16:57:51.645Z", "user_id": 348,
 "session_id": "c4efb608a0d5", "movie_id": 2, "event_type": "rate", "rating": 2.5,
 "watch_seconds": null, "device": "tablet", "city": "Ha Noi", "is_new_user": false}
```

| event_type | Tỉ lệ | Ý nghĩa |
|---|---|---|
| view | 45% | mở trang phim |
| click_trailer | 15% | xem trailer |
| add_to_watchlist | 10% | thêm vào danh sách xem sau |
| watch_complete | 10% | xem hết phim |
| rate | 20% | chấm điểm, bổ sung trực tiếp vào bảng ratings |

---

## Chương 4. Thiết kế hệ thống

### 4.1. Kiến trúc tổng thể
**[ẢNH]**: sơ đồ kiến trúc (có thể vẽ lại từ sơ đồ trong README.md).

```
MovieLens CSV ─► HDFS ─(query)─► Spark SQL ─► Bảng phân tích
                  ▲                  └──────► Spark MLlib ALS ─► Mô hình gợi ý
                  │
Script mô phỏng ─► Kafka ─► Spark Structured Streaming (làm giàu) ─┘
                 Airflow điều phối toàn bộ
```

### 4.2. Triển khai (Docker Compose)

| Container | Image | Cổng |
|---|---|---|
| namenode, datanode | bde2020/hadoop-*:3.2.1 | 9870, 9000, 9864 |
| kafka, kafka-ui | apache/kafka:3.7.0, provectuslabs/kafka-ui | 29092, 8085 |
| spark-master, spark-worker | apache/spark:3.5.1 (+ connector Kafka) | 8080, 7077, 8081, 4040 |
| airflow, postgres | apache/airflow:2.9.3, postgres:15 | 8088 |

### 4.3. Tổ chức data lake trên HDFS

| Vùng | Đường dẫn | Định dạng |
|---|---|---|
| Raw | `/datalake/raw/movielens/` | CSV |
| Streaming | `/datalake/streaming/events/` (phân vùng `event_date`), `/datalake/streaming/trending/` | Parquet |
| Curated | `/datalake/curated/ratings_all/` | Parquet |
| Analytics | `/datalake/analytics/<bảng>/` | Parquet |
| Model | `/models/als/latest`, `/models/als/<phiên bản>` | Spark ML model |

### 4.4. Các luồng (DAG Airflow)

| DAG | Các task | Mô tả |
|---|---|---|
| `01_ingest_movielens_to_hdfs` | download_dataset → wait_hdfs_ready → create_hdfs_dirs → put_files_to_hdfs → verify_hdfs | Đưa dữ liệu gốc vào HDFS |
| `02_streaming_user_events` | create_kafka_topic → [spark_streaming_job ‖ generate_user_events] → check_streaming_output | Sinh sự kiện → Kafka → Spark Streaming → HDFS (2 task chạy song song) |
| `03_batch_analytics_and_ml` | check_raw_data_on_hdfs → spark_batch_analytics → train_als_model → show_results | Truy vấn, phân tích, huấn luyện mô hình |
| `00_full_pipeline` | run_01 → run_02 → run_03 | Chạy toàn bộ theo thứ tự |

**[ẢNH]**: Graph view của từng DAG trong Airflow.

### 4.5. Xử lý luồng (Spark Structured Streaming)
1. Đọc topic `movie-events` (lần đầu từ `earliest`, các lần sau đọc tiếp theo offset lưu trong checkpoint).
2. Parse JSON theo schema, chuyển `event_time` sang timestamp.
3. **Làm giàu**: stream-static join (broadcast) với bảng phim để thêm `title`, `genres`, `primary_genre`, `release_year`.
4. Sink 1: ghi Parquet vào `/datalake/streaming/events`, trigger 20 giây.
5. Sink 2: gom nhóm theo **cửa sổ 1 phút** và phim, **watermark 30 giây**, tính số sự kiện, lượt xem,
   điểm TB, số người dùng; ghi `/datalake/streaming/trending` và in top 10 ra màn hình.

### 4.6. Xử lý lô và học máy
- `batch_analytics.py`: gộp ratings gốc với ratings mới từ streaming (cột `source`), sau đó chạy các truy vấn Spark SQL (mục 5.2).
- `train_model.py`: chia 80/20, grid search `rank ∈ {10, 20}`, `regParam ∈ {0.05, 0.1}`, chọn mô hình có RMSE thấp nhất,
  huấn luyện lại trên toàn bộ dữ liệu, lưu mô hình và sinh top-10 gợi ý cho mọi người dùng.

---

## Chương 5. Thực nghiệm và kết quả

### 5.1. Vận hành pipeline
- **[ẢNH]** `docker compose ps`: 8 container đang chạy.
- **[ẢNH]** HDFS UI: thư mục `/datalake/raw/movielens`.
- **[ẢNH]** Kafka UI: topic `movie-events`, số message, nội dung một message.
- **[ẢNH]** Log task `spark_streaming_job`: bảng *Top 10 phim trending*.
- **[ẢNH]** Spark UI (http://localhost:4041), tab *Structured Streaming*: biểu đồ input rate / processing rate.
- **[ẢNH]** Airflow: DAG `00_full_pipeline` chạy thành công (toàn bộ task màu xanh).

### 5.2. Kết quả phân tích (thư mục `output/analytics/`)

| Bảng | Câu hỏi trả lời |
|---|---|
| dataset_overview | Số ratings / người dùng / phim theo nguồn (historical vs streaming) |
| top_rated_movies | 20 phim điểm cao nhất (≥ 50 lượt đánh giá) |
| most_popular_movies | 20 phim được đánh giá nhiều nhất |
| genre_stats | Số phim, số lượt đánh giá, điểm TB theo thể loại |
| ratings_by_decade | Điểm TB theo thập niên phát hành |
| ratings_per_year | Số lượt đánh giá, người dùng hoạt động theo năm |
| rating_distribution | Phân bố điểm số |
| top_active_users, top_tags | Người dùng tích cực nhất, tag phổ biến |
| stream_event_types, stream_city_device | Hành vi theo loại sự kiện, thành phố, thiết bị |
| stream_genre_funnel | Phễu chuyển đổi view → watch_complete theo thể loại |
| stream_events_per_minute, stream_trending_movies | Lưu lượng sự kiện theo phút, phim trending |

**[SỐ LIỆU]**: chèn bảng / biểu đồ từ các file CSV trên (vẽ bằng Excel). Nhận xét gợi ý:
thể loại nhiều lượt đánh giá nhất (thường là Drama, Comedy), thể loại điểm TB cao (Film-Noir, War, Documentary),
phân bố điểm lệch về 3.5 – 4.0, phim kinh điển (thập niên 1940 – 1970) có điểm TB cao hơn.

### 5.3. Kết quả mô hình gợi ý (`output/model/`)

| rank | regParam | RMSE | MAE |
|---|---|---|---|
| 10 | 0.05 | **[SỐ LIỆU]** | |
| 10 | 0.10 | | |
| 20 | 0.05 | | |
| 20 | 0.10 | | |

- Mô hình tốt nhất: **[SỐ LIỆU]** (`metrics.json`). Trên MovieLens small, ALS thường đạt RMSE khoảng 0.87 – 0.90.
- Ví dụ top-10 phim gợi ý cho người dùng 1: **[SỐ LIỆU]** (`sample_recommendations.csv`).
- `training_history.csv`: so sánh các lần huấn luyện khi số ratings từ streaming tăng dần.

---

## Chương 6. Kết luận và hướng phát triển

### 6.1. Kết quả đạt được
- Dựng được cụm Big Data đầy đủ HDFS – Kafka – Spark – Airflow bằng Docker.
- Pipeline batch: dữ liệu lưu HDFS → Spark SQL truy vấn → bảng phân tích + mô hình gợi ý.
- Pipeline streaming: script sinh dữ liệu liên tục → Kafka → Spark Structured Streaming → làm giàu → HDFS.
- Dữ liệu streaming quay lại làm giàu tập huấn luyện, khép kín vòng lặp dữ liệu.
- Airflow điều phối, theo dõi, chạy lại các luồng.

### 6.2. Hạn chế
- Chạy trên một máy, HDFS replication = 1.
- Dữ liệu luồng là mô phỏng.
- Chưa có dashboard trực quan thời gian thực.

### 6.3. Hướng phát triển
- Thêm dashboard (Superset / Grafana) đọc kết quả trending.
- Phục vụ gợi ý thời gian thực (API đọc mô hình ALS), dùng thêm tín hiệu ngầm (view, watchlist) với `implicitPrefs`.
- Triển khai trên nhiều node / Kubernetes, dùng Delta Lake / Iceberg cho vùng curated.

## Tài liệu tham khảo
1. F. Maxwell Harper and Joseph A. Konstan. 2015. *The MovieLens Datasets: History and Context.* ACM TiiS 5(4).
2. Apache Spark Documentation 3.5: Structured Streaming Programming Guide, MLlib Collaborative Filtering.
3. Apache Kafka Documentation 3.7.
4. Apache Airflow Documentation 2.9.
5. Apache Hadoop 3.2 HDFS Architecture Guide.
