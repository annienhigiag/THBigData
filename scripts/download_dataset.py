"""
Tải bộ dữ liệu MovieLens (ml-latest-small, GroupLens) về thư mục data/movielens.

  - ratings.csv : ~100.000 lượt đánh giá (userId, movieId, rating, timestamp)
  - movies.csv  : ~9.700 phim (movieId, title, genres)
  - tags.csv    : ~3.600 tag người dùng gắn cho phim
  - links.csv   : liên kết IMDB / TMDB

Nguồn: https://grouplens.org/datasets/movielens/latest/

Chạy:  python scripts/download_dataset.py [--out data/movielens]
"""
import argparse
import io
import os
import shutil
import sys
import urllib.request
import zipfile

URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
FILES = ["ratings.csv", "movies.csv", "tags.csv", "links.csv"]


def main():
    default_out = os.path.join(os.environ.get("PROJECT_DATA_DIR", "data"), "movielens")
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=default_out)
    parser.add_argument("--force", action="store_true", help="tải lại kể cả khi đã có")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    if not args.force and all(os.path.exists(os.path.join(args.out, f)) for f in FILES):
        print(f"Dữ liệu đã có sẵn trong {args.out}, bỏ qua bước tải.")
        return

    print(f"Đang tải {URL} ...")
    with urllib.request.urlopen(URL, timeout=120) as resp:
        payload = resp.read()
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        for name in FILES:
            with zf.open(f"ml-latest-small/{name}") as src, open(os.path.join(args.out, name), "wb") as dst:
                shutil.copyfileobj(src, dst)
            print(f"  -> {os.path.join(args.out, name)}")
    print("Xong.")


if __name__ == "__main__":
    sys.exit(main())
