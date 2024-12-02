FROM python:3.9-slim

WORKDIR /app/sql_analytics

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install jupyter nbconvert

COPY . .

RUN jupyter nbconvert --to python sql_analytics_notebook.ipynb \
    && mv sql_analytics_notebook.py main.py

CMD ["python", "main.py"]
