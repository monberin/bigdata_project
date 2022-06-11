FROM datamechanics/spark:3.1-latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

RUN pip install cassandra-driver
RUN pip install pyspark
RUN pip install apscheduler

COPY ./spark_processing.py .

CMD ["python3", "spark_processing.py"]
