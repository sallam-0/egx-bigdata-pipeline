FROM apache/spark:3.4.1

USER root

# Install pip and Python dependencies required to run pipeline scripts
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3-pip \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    pyspark==3.4.1 \
    pandas \
    pyarrow \
    kafka-python \
    python-dotenv

USER spark
