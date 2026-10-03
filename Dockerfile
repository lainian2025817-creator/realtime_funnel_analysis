FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir mysql-connector-python

USER spark
