FROM python:3.9-slim

WORKDIR /app/data_ingestion

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install jupyter nbconvert

COPY . .

RUN jupyter nbconvert --to python data_ingestion_notebook.ipynb \
    && mv data_ingestion_notebook.py main.py

CMD ["python", "main.py"]
