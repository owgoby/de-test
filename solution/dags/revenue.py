"""
### Витрина `mart.revenue`, 6 раз в сутки

Источник MariaDB (реплика прода) -> `raw.*` -> `stg.*` -> `mart.revenue`.

1. `ddl`: базы, таблицы и пользователи ClickHouse, все запросы идемпотентны.
2. `load_<таблица>`: восемь таблиц параллельно, каждая своей стратегией
   (маленькие целиком, большие инкрементом по `id`).
3. `reread_pending`: платежи, которые в `raw` ещё `pending`, перечитываются по `id`.
4. `reconcile_<таблица>`: сверка `orders` и `payments` по бакетам `id`, ловит удаления
   мошеннических заказов и отзыв платежей.
5. `build_mart`: пересборка в `mart.revenue_new`, блокирующие проверки, `EXCHANGE TABLES`.
   Если проверка не прошла, задача падает, аналитики видят прежнюю витрину.
6. `check_mart`: инварианты опубликованной витрины, результаты в `meta.check_results`.

Подробности в README репозитория.
"""
from datetime import datetime

from airflow.sdk import chain, dag, task

from common import DEFAULT_ARGS, RAW_POOL, run_id, settings
from pipeline import ch, checks, ddl, load, mart


@task
def apply_ddl():
    s = settings()
    ddl.apply_ddl(ch.ch_admin(s), s.ddl_vars())


@task(pool=RAW_POOL)
def load_table(table: str):
    load.load_table(settings(), table, run_id())


@task(pool=RAW_POOL)
def reread_pending():
    load.reread_pending(settings(), run_id())


@task(pool=RAW_POOL)
def reconcile(table: str):
    load.reconcile_table(settings(), table, run_id())


# Повтор не лечит провал блокирующей проверки, нужен разбор. В pool: читает raw через stg,
# поэтому аудит не должен менять raw посреди сборки. Сверки витрины со stg идут здесь же, до публикации.
@task(retries=0, pool=RAW_POOL)
def build_mart():
    mart.build_revenue(settings(), run_id())


# Вне pool: читает только опубликованную mart.revenue, а её меняет лишь build_mart этого же DAG
# (max_active_runs=1). Поэтому audit, начавшийся после сборки, проверку не ломает.
@task(retries=0)
def check_mart():
    if not checks.run_checks(settings(), run_id()):
        raise RuntimeError("инварианты витрины не прошли, подробности в meta.check_results")


@dag(
    dag_id="revenue",
    doc_md=__doc__,
    schedule="0 */4 * * *",       # 6 раз в сутки
    start_date=datetime(2026, 11, 1),
    catchup=False,                # пропущенные интервалы не догоняются: каждый прогон берёт всё новое
    max_active_runs=1,            # два прогона одновременно не пишут в raw и не подменяют витрину
    default_args=DEFAULT_ARGS,
    tags=["revenue", "clickhouse"],
)
def revenue():
    loads = {t: load_table.override(task_id=f"load_{t}")(t) for t in [*load.SMALL_TABLES, *load.BIG_TABLES]}
    chain(apply_ddl(), list(loads.values()))

    # payments: инкремент -> перечитка pending -> сверка; orders: инкремент -> сверка
    reconcile_payments = reconcile.override(task_id="reconcile_payments")("payments")
    chain(loads["payments"], reread_pending(), reconcile_payments)
    reconcile_orders = reconcile.override(task_id="reconcile_orders")("orders")
    chain(loads["orders"], reconcile_orders)

    chain([*loads.values(), reconcile_payments, reconcile_orders], build_mart(), check_mart())


revenue()
