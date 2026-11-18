"""Замер на 100-кратном объёме прямо в ClickHouse.

Данные raw размножаются со сдвигом ключей, затем замеряются чтение stg через FINAL и пересборка
витрины. После замера загрузку на этом стенде запускать нельзя: сверка пометит лишние строки
удалёнными. Поэтому тест не входит в обычный запуск (маркер scale), а стенд потом возвращают make reset.
"""
import time

import pytest

pytestmark = pytest.mark.scale

COPIES = 99           # к исходным данным добавляется 99 копий: всего x100
SHIFT = 10_000_000    # сдвиг id и user_id для каждой копии, больше любого id стенда
REBUILD_BUDGET_S = 600  # прогон раз в 4 часа; пересборка должна занимать минуты, а не часы

CROSS = f"CROSS JOIN (SELECT number + 1 AS k FROM numbers({COPIES})) AS n"
MULTIPLY = {
    "users": f"SELECT id + k*{SHIFT}, country, is_tester, is_staff, created_at, _version, _deleted, _loaded_at",
    "payments": f"SELECT id + k*{SHIFT}, concat(payment_uid, '-', toString(k)), user_id + k*{SHIFT}, "
                "payment_system_id, status, amount_eur, created_at, _version, _deleted, _loaded_at",
    "orders": f"SELECT id + k*{SHIFT}, user_id + k*{SHIFT}, "
              "if(payment_uid IS NULL, NULL, concat(payment_uid, '-', toString(k))), "
              "plan_id, points_used, platform, created_at, _version, _deleted, _loaded_at",
    "activations": f"SELECT id + k*{SHIFT}, order_id + k*{SHIFT}, user_id + k*{SHIFT}, activated_at, "
                   "_version, _deleted, _loaded_at",
    "usage_daily": f"SELECT id + k*{SHIFT}, activation_id + k*{SHIFT}, usage_date, usage_units, cost_eur, "
                   "_version, _deleted, _loaded_at",
    "refunds": f"SELECT id + k*{SHIFT}, concat(payment_uid, '-', toString(k)), amount_eur, created_at, "
               "_version, _deleted, _loaded_at",
}


def timed(fn) -> float:
    t0 = time.monotonic()
    fn()
    return time.monotonic() - t0


def test_rebuild_on_100x_volume(stand, monkeypatch):
    # Данные размножены только в ClickHouse: сравнение с прошлой версией и с источником здесь заведомо
    # не сходится, поэтому замер идёт без него. Остальные блокирующие проверки работают.
    monkeypatch.setenv("MART_BASELINE_CHECKS", "0")
    if stand.ch_value("SELECT count() FROM raw.orders") == 0:  # свежий стенд: сначала обычная загрузка
        stand.pipeline()
    orders_before = stand.ch_value("SELECT count() FROM stg.orders")

    for table, select in MULTIPLY.items():
        stand.ch.command(f"INSERT INTO raw.{table} {select} FROM raw.{table} {CROSS}")
    assert stand.ch_value("SELECT count() FROM stg.orders") == orders_before * (COPIES + 1)

    timings = {
        "stg.usage_daily, count через FINAL":
            timed(lambda: stand.ch_value("SELECT count() FROM stg.usage_daily")),
        "stg.orders JOIN stg.users, оба через FINAL":
            timed(lambda: stand.ch_value("SELECT count() FROM stg.orders o INNER JOIN stg.users u ON u.id = o.user_id")),
        "пересборка mart.revenue с проверками и подменой":
            timed(lambda: stand.pipeline("mart")),
    }
    print(f"\nобъём: заказов {stand.ch_value('SELECT count() FROM stg.orders')}, "
          f"строк расхода {stand.ch_value('SELECT count() FROM stg.usage_daily')}, "
          f"витрина {stand.ch_value('SELECT count() FROM mart.revenue')} строк")
    for name, seconds in timings.items():
        print(f"{name}: {seconds:.2f} с")

    assert timings["пересборка mart.revenue с проверками и подменой"] < REBUILD_BUDGET_S
