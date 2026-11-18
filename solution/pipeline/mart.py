"""Пересборка витрины mart.revenue из solution/sql/revenue_build.sql.

Порядок: собрать revenue_new -> блокирующие проверки -> EXCHANGE TABLES.
Если проверки не прошли, старая витрина остаётся на месте, а прогон падает:
аналитики не увидят сломанную финансовую витрину.

Блокируют публикацию только проверки, ошибка в которых даёт неверные цифры:
- структура и бизнес-правила новой таблицы (все инварианты из checks.py, в том числе сверка со stg,
  плюс непустота и план);
- число строк против прошлой версии: падение больше 2% или рост больше 10%;
- сумма success-платежей за 7 дней в stg против источника: расхождение больше 0,5%.
Свежесть и объём загрузки публикацию не блокируют: устаревшие данные не неверные.

Результат каждой блокирующей проверки пишется в meta.check_results с префиксом «до публикации: »
до решения о публикации: если витрина не опубликована, там видно, какая проверка не прошла."""
import logging
import os
import uuid
from datetime import timedelta
from pathlib import Path

from pipeline import ch
from pipeline.checks import MART_INVARIANTS, STAGE_INVARIANTS, record_results
from pipeline.config import Settings, as_utc
from pipeline.ddl import statements

log = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
CHECK_PREFIX = "до публикации: "

# Структура новой витрины: имя -> SQL, возвращающий число нарушений; {t} = mart.revenue_new.
BLOCKING_CHECKS: dict[str, str] = {
    "витрина не пуста": "SELECT if(count() = 0, 1, 0) FROM {t}",
    # NOT IN, а не LEFT JOIN ... WHERE p.id = 0: не зависит от настройки join_use_nulls.
    "у каждого заказа есть план": "SELECT count() FROM {t} WHERE plan_id NOT IN (SELECT id FROM stg.plans)",
    **MART_INVARIANTS,
    **STAGE_INVARIANTS,
}

# Число строк против прошлой версии. Абсолютный минимум нужен, чтобы на маленьком наборе данных
# обычный пакет изменений не срабатывал как аномалия.
MAX_DROP, MAX_GROWTH, MIN_ROWS_DELTA = 0.02, 0.10, 100
# Выручка против источника: окно по индексу created_at в MariaDB, пороги относительного расхождения.
REVENUE_WINDOW_DAYS, REVENUE_FAIL, REVENUE_WARN = 7, 0.005, 0.001


def _row_delta(client) -> tuple[bool, str]:
    """Число строк новой витрины против опубликованной: (прошла ли, что измерено)."""
    prev = client.query("SELECT count() FROM mart.revenue").result_rows[0][0]
    new = client.query("SELECT count() FROM mart.revenue_new").result_rows[0][0]
    if prev == 0:  # первая сборка: сравнивать не с чем
        return True, f"{new} строк, первая сборка"
    delta = new - prev
    change = delta / prev
    passed = abs(delta) < MIN_ROWS_DELTA or -MAX_DROP <= change <= MAX_GROWTH
    return passed, f"{prev} -> {new} ({change:+.1%})"


def _revenue_vs_source(client, s: Settings) -> tuple[bool, str]:
    """Сумма success-платежей за последние 7 дней по часам источника, в stg и в MariaDB:
    (прошла ли, что измерено). В MariaDB это диапазон по индексу created_at, большая таблица
    целиком не читается."""
    conn = ch.mariadb(s)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(created_at) FROM payments")
            clock = cur.fetchone()[0]
            if clock is None:
                return True, "в источнике нет платежей"
            since = clock - timedelta(days=REVENUE_WINDOW_DAYS)
            cur.execute("SELECT COALESCE(SUM(amount_eur), 0) FROM payments "
                        "WHERE status = 'success' AND created_at >= %s", (since,))
            src = cur.fetchone()[0]
    finally:
        conn.close()
    # Пояс явный: clickhouse-connect переводит параметр в пояс сервера через astimezone(),
    # и время без пояса посчитал бы местным временем процесса.
    dst = client.query(
        "SELECT sum(amount_eur) FROM stg.payments WHERE status = 'success' AND created_at >= {since:DateTime}",
        parameters={"since": as_utc(since)},
    ).result_rows[0][0]
    diff = abs(float(dst) - float(src)) / float(src) if src else (0.0 if not dst else 1.0)
    measured = f"хранилище {dst}, источник {src} ({diff:.2%})"
    if REVENUE_WARN < diff <= REVENUE_FAIL:
        log.warning("выручка за %d дней расходится с источником на %.2f%%: %s против %s",
                    REVENUE_WINDOW_DAYS, diff * 100, dst, src)
    return diff <= REVENUE_FAIL, measured


def build_revenue(s: Settings, run_id: uuid.UUID | None = None) -> int:
    client = ch.ch_loader(s)
    run_id = run_id or uuid.uuid4()
    for statement in statements((SQL_DIR / "revenue_build.sql").read_text(encoding="utf-8")):
        client.command(statement)
    results: list[tuple[str, bool, object, object]] = []
    for name, sql in BLOCKING_CHECKS.items():
        violations = client.query(sql.replace("{t}", "mart.revenue_new")).result_rows[0][0]
        results.append((name, violations == 0, violations, 0))
    # Сравнение с прошлой версией и с источником. Выключается только для синтетических замеров
    # (tests/test_scale100.py размножает данные внутри ClickHouse, источник при этом не меняется).
    if os.environ.get("MART_BASELINE_CHECKS", "1") != "0":
        passed, measured = _row_delta(client)
        results.append(("число строк против прошлой версии", passed, measured,
                        f"падение до {MAX_DROP:.0%}, рост до {MAX_GROWTH:.0%} "
                        f"или изменение меньше {MIN_ROWS_DELTA} строк"))
        passed, measured = _revenue_vs_source(client, s)
        results.append((f"выручка за {REVENUE_WINDOW_DAYS} дней против источника", passed, measured,
                        f"расхождение до {REVENUE_FAIL:.1%}"))
    # Записывается до решения о публикации: при провале в meta видно, что именно не прошло.
    record_results(client, run_id, [(CHECK_PREFIX + name, *rest) for name, *rest in results])
    failed = [f"{name} (измерено {measured})" for name, passed, measured, _ in results if not passed]
    if failed:
        raise RuntimeError(f"витрина не опубликована, старая версия оставлена: {failed}")
    client.command("EXCHANGE TABLES mart.revenue AND mart.revenue_new")
    client.command("DROP TABLE mart.revenue_new")
    rows = client.query("SELECT count() FROM mart.revenue").result_rows[0][0]
    log.info("mart.revenue пересобрана и опубликована: %d строк", rows)
    return rows


def export_revenue(s: Settings, name: str) -> Path:
    """Выгрузка витрины в CSV с заголовком, порядок колонок как в таблице, сортировка по order_id.
    Пустые значения (NULL) пишутся пустым полем, десятичные с двумя знаками."""
    client = ch.ch_loader(s)
    data = client.raw_query(
        "SELECT * FROM mart.revenue ORDER BY order_id",
        fmt="CSVWithNames",
        settings={"format_csv_null_representation": "", "output_format_decimal_trailing_zeros": 1},
    )
    out = Path(s.results_dir) / f"revenue_{name}.csv"
    out.write_bytes(data)
    log.info("выгрузка: %s (%d байт)", out, len(data))
    return out
