"""Общее для DAG витрины: настройки пайплайна из Airflow Connections и run_id запуска.

Логика загрузки и сборки живёт в пакете pipeline; DAG только задаёт порядок шагов,
повторы и расписание. Поэтому тот же код запускается и из CLI (`python -m pipeline`)."""
import json
import uuid
from datetime import timedelta
from pathlib import Path

from airflow.sdk import Connection, get_current_context

from pipeline.config import Settings

# Шаги идемпотентны (ReplacingMergeTree, водяной знак = max(id) в raw, витрина через EXCHANGE),
# поэтому повтор упавшей задачи безопасен.
DEFAULT_ARGS = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Pool доступа к raw (solution/airflow_pools.json, импортируется при старте Airflow).
# Версия строки назначается в начале задачи, поэтому две задачи, пишущие одну таблицу одновременно,
# могли бы записать более старое чтение источника с большей версией. Задачи revenue берут по 1 слоту
# и идут параллельно, аудит берёт все слоты: он не пересекается ни с загрузкой, ни со сборкой витрины.
RAW_POOL = "raw_access"
RAW_POOL_SLOTS = json.loads(
    (Path(__file__).resolve().parent.parent / "airflow_pools.json").read_text(encoding="utf-8")
)[RAW_POOL]["slots"]


def settings() -> Settings:
    return Settings.from_connections(
        mariadb=Connection.get("mariadb_app"),
        ch_admin=Connection.get("clickhouse_admin"),
        ch_loader=Connection.get("clickhouse_loader"),
        ch_analyst=Connection.get("clickhouse_analyst"),
    )


def run_id() -> uuid.UUID:
    """Один UUID на запуск DAG: все шаги запуска пишутся в meta.load_runs и meta.check_results
    под ним, и запуск в UI Airflow сопоставляется со строками meta по этому значению."""
    ctx = get_current_context()
    return uuid.uuid5(uuid.NAMESPACE_URL, f"airflow/{ctx['dag'].dag_id}/{ctx['run_id']}")
