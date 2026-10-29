FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
# static/vendor chứa sẵn Chart.js 4.4.1 nên trang web chạy được cả khi không có Internet
COPY static/ /app/static/
EXPOSE 8501
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8501"]
