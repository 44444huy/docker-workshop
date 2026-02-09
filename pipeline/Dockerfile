FROM python:3.13-slim

# Cài dependencies
RUN pip install \
    pandas \
    pyarrow \
    sqlalchemy \
    psycopg[binary] \
    click \
    tqdm

# Thư mục làm việc trong container
WORKDIR /app

# Copy file ingest
COPY ingest_data.py ingest_data.py

# Khi docker run → chạy script này
ENTRYPOINT ["python", "ingest_data.py"]
