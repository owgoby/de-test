"""Структура DAG: загружаются без ошибок, расписание и зависимости такие, как описано в README.

Запускается в контейнере Airflow (там есть airflow), не требует данных и подключений:
    docker compose exec airflow pytest solution/tests/dags
"""
import json
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="тесты DAG идут в контейнере Airflow")
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
TABLES = ["users", "plans", "payment_systems", "payments", "orders", "activations", "usage_daily", "refunds"]


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    # Как планировщик: папка DAG в sys.path, поэтому DAG импортируют общий модуль common.
    with pytest.MonkeyPatch.context() as mp:
        mp.syspath_prepend(str(DAGS_DIR))
        return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"revenue", "revenue_audit"}


def test_revenue_schedule(dagbag):
    dag = dagbag.get_dag("revenue")
    assert dag.timetable.expression == "0 */4 * * *"   # 6 раз в сутки
    assert dag.max_active_runs == 1                  # прогоны не пересекаются
    assert dag.catchup is False


def upstream(dag, task_id: str) -> set[str]:
    return set(dag.get_task(task_id).upstream_task_ids)


def test_revenue_dependencies(dagbag):
    dag = dagbag.get_dag("revenue")
    for table in TABLES:
        assert upstream(dag, f"load_{table}") == {"apply_ddl"}
    assert upstream(dag, "reread_pending") == {"load_payments"}
    assert upstream(dag, "reconcile_payments") == {"reread_pending"}
    assert upstream(dag, "reconcile_orders") == {"load_orders"}
    assert upstream(dag, "build_mart") == {f"load_{t}" for t in TABLES} | {"reconcile_payments", "reconcile_orders"}
    assert upstream(dag, "check_mart") == {"build_mart"}


def test_mart_tasks_are_not_retried(dagbag):
    """Провал блокирующей проверки повтором не лечится; загрузки идемпотентны и повторяются."""
    dag = dagbag.get_dag("revenue")
    assert dag.get_task("build_mart").retries == 0
    assert dag.get_task("check_mart").retries == 0
    assert all(dag.get_task(f"load_{t}").retries > 0 for t in TABLES)


def test_audit_schedule(dagbag):
    dag = dagbag.get_dag("revenue_audit")
    assert dag.timetable.expression == "0 2 * * *"
    assert {t.task_id for t in dag.tasks} == {"audit_orders", "audit_payments"}


RAW_TASKS = [f"load_{t}" for t in TABLES] + ["reread_pending", "reconcile_payments", "reconcile_orders", "build_mart"]


def test_raw_tasks_share_pool(dagbag):
    """Все задачи revenue, которые пишут в raw или читают его для витрины, в одном pool по 1 слоту."""
    dag = dagbag.get_dag("revenue")
    for task_id in RAW_TASKS:
        task = dag.get_task(task_id)
        assert (task.pool, task.pool_slots) == ("raw_access", 1), task_id
    assert dag.get_task("apply_ddl").pool != "raw_access"
    assert dag.get_task("check_mart").pool != "raw_access"


def test_audit_takes_whole_pool(dagbag):
    """Аудит занимает все слоты: не пересекается с revenue. Слотов хватает на все загрузки параллельно."""
    slots = json.loads((DAGS_DIR.parent / "airflow_pools.json").read_text(encoding="utf-8"))["raw_access"]["slots"]
    assert slots == len(TABLES)
    for task in dagbag.get_dag("revenue_audit").tasks:
        assert (task.pool, task.pool_slots) == ("raw_access", slots), task.task_id
