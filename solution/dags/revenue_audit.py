"""
### Ночная полная сверка `orders` и `payments`

Штатный прогон `revenue` хеширует статус платежа только за последние 30 дней.
Раз в сутки этот DAG сверяет хеш по всей таблице: так ловится отзыв старого платежа
(`success -> declined`) и любое другое расхождение с источником.

Исправления пишутся в `raw`; витрину пересоберёт ближайший прогон `revenue`.
Время 02:00 выбрано между прогонами `revenue` (00:00 и 04:00). Если прогон затянется, они всё равно
не пересекутся: аудит занимает все слоты pool `raw_access`, а задачи `revenue` по одному.
"""
from datetime import datetime

from airflow.sdk import dag, task

from common import DEFAULT_ARGS, RAW_POOL, RAW_POOL_SLOTS, run_id, settings
from pipeline import load


# Все слоты pool: аудит идёт, только когда revenue не трогает raw, и не пускает его, пока идёт сам.
@task(pool=RAW_POOL, pool_slots=RAW_POOL_SLOTS)
def audit(table: str):
    load.reconcile_table(settings(), table, run_id(), full=True)


@dag(
    dag_id="revenue_audit",
    doc_md=__doc__,
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["revenue", "clickhouse"],
)
def revenue_audit():
    for table in load.RECONCILE_TABLES:
        audit.override(task_id=f"audit_{table}")(table)


revenue_audit()
