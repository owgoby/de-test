FROM apache/airflow:3.3.2-python3.12
# В образе Airflow уже есть pandas и PyMySQL; добавлены клиент ClickHouse и pytest для тестов DAG.
# Airflow закреплён в той же команде, чтобы pip не сдвинул его зависимости.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "clickhouse-connect==0.8.17" "pytest==8.3.4"
