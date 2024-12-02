FROM python:3.9-slim

WORKDIR /app/data_preprocessing_cleaning

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install jupyter nbconvert

COPY . .

RUN jupyter nbconvert --to python data_preprocessing_notebook.ipynb \
    && mv data_preprocessing_notebook.py main.py

CMD ["python", "main.py"]
