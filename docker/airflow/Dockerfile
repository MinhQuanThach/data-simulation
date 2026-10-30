ARG SPARK_IMAGE=apache/spark:4.0.2-scala2.13-java17-python3-ubuntu@sha256:17dcbf3ceb10994198bd32ab8cf2944072ebb865acd00b76c6ece17ed391ffff
ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.11@sha256:4158d6893315cf7c7a81a99f423b7b443dd5454a003f87f4cb0a72748e850978
FROM ${SPARK_IMAGE} AS spark
FROM ${AIRFLOW_IMAGE}
USER root
COPY --from=spark /opt/spark /opt/spark
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/java/openjdk/bin:${PATH}
ENV PYSPARK_PYTHON=/usr/local/bin/python PYSPARK_DRIVER_PYTHON=/usr/local/bin/python
ENV PYTHONPATH=/opt/lab SPARK_LOCAL_DIRS=/tmp/spark-local
COPY requirements/airflow.in requirements/constraints-project-py311.txt /opt/lab-build/requirements/
COPY docker/airflow/fetch_jdbc.py /opt/lab-build/fetch_jdbc.py
COPY docker/airflow/fetch_pyspark.py /opt/lab-build/fetch_pyspark.py
RUN python /opt/lab-build/fetch_jdbc.py && chmod -R a+rX /opt/spark /opt/java/openjdk
USER airflow
RUN python /opt/lab-build/fetch_pyspark.py \
    && python -m pip install --no-cache-dir --constraint /opt/lab-build/requirements/constraints-project-py311.txt --requirement /opt/lab-build/requirements/airflow.in /tmp/pyspark-4.0.2.tar.gz \
    && rm /tmp/pyspark-4.0.2.tar.gz \
    && python -m pip check \
    && python -c "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator; import pyspark; assert pyspark.__version__ == '4.0.2'" \
    && java -version && spark-submit --version \
    && python -m pip freeze --all > /opt/airflow/runtime-freeze.txt
