FROM apache/airflow:2.11.2

# Keep ingestion dependencies available when this container is recreated.
RUN pip install --no-cache-dir \
    "apache-airflow==2.11.2" \
    "boto3==1.42.61" \
    "pyarrow==22.0.0" \
    "snowflake-connector-python==4.3.0" \
    "virtualenv==21.1.0"
