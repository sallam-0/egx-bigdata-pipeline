FROM apache/airflow:2.8.1-python3.11

USER root
# Install default-jre for PySpark + docker CLI to exec into other containers
RUN apt-get update \
  && apt-get install -y --no-install-recommends default-jre-headless docker.io \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
# Install PySpark and all necessary Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    yfinance \
    pandas \
    pyarrow \
    kafka-python \
    python-dotenv
