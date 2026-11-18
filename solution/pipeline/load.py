"""Загрузка таблиц источника (MariaDB) в слой raw ClickHouse.

Две стратегии, обе идемпотентны: повторный прогон ничего не дублирует,
потому что raw-таблицы лежат на ReplacingMergeTree(_version, _deleted) ORDER BY id.

1. Маленькие таблицы (users, plans, payment_systems): полная перезагрузка.
   Таблица целиком читается в raw.<t>_new и атомарно подменяется EXCHANGE TABLES.
   Так сами собой приходят любые изменения и удаления, включая пометку
   пользователя тестовым задним числом.

2. Большие таблицы (payments, orders, activations, usage_daily, refunds):
   инкремент по id. У всех таблиц id AUTO_INCREMENT, поэтому новые строки,
   в том числе опоздавшие платежи и расход за прошлые дни, всегда имеют
   id больше уже загруженного. Читаются только они, с запасом LOAD_OVERLAP_IDS id
   назад (по умолчанию 1000): в проде транзакция с меньшим id может
   зафиксироваться позже.

   Поверх инкремента два механизма для строк, которые меняются после вставки,
   и ни один из них не читает большую таблицу целиком:
   - платежи, которые в raw ещё pending, перечитываются по id, какого бы
     возраста они ни были: их статус рано или поздно изменится;
   - сверка по бакетам id (orders, payments): источник удаляет мошеннические
     заказы вместе с платежом. По бакетам сравниваются число строк и сумма id,
     это обход вторичного индекса без чтения строк. Для payments дополнительно
     хеш статуса, но только по свежему диапазону id (RECONCILE_HASH_DAYS):
     статус меняется в течение 72 часов, а хеш требует читать колонку status.
     Полный хеш по всей таблице выполняет отдельная команда `audit`
     (в проде раз в сутки). Расходящиеся бакеты разбираются по id: исчезнувшие
     строки получают маркер удаления, изменённые перечитываются.

Версия строки одна на весь прогон и не меньше уже записанной + 1, поэтому любая
строка, прочитанная позже, побеждает прочитанную раньше.
"""
import logging
import time
import uuid
from datetime import datetime, timedelta

from pipeline import ch
from pipeline.config import Settings, as_utc, utc_now

log = logging.getLogger(__name__)

# Колонки в порядке источника. Служебные колонки raw добавляются при вставке.
SMALL_TABLES: dict[str, list[str]] = {
    "users": ["id", "country", "is_tester", "is_staff", "created_at"],
    "plans": ["id", "name", "size_gb", "days", "is_unlimited", "price_eur"],
    "payment_systems": ["id", "name"],
}
BIG_TABLES: dict[str, list[str]] = {
    "payments": ["id", "payment_uid", "user_id", "payment_system_id", "status", "amount_eur", "created_at"],
    "orders": ["id", "user_id", "payment_uid", "plan_id", "points_used", "platform", "created_at"],
    "activations": ["id", "order_id", "user_id", "activated_at"],
    "usage_daily": ["id", "activation_id", "usage_date", "usage_units", "cost_eur"],
    "refunds": ["id", "payment_uid", "amount_eur", "created_at"],
}
# Таблицы, где источник удаляет строки, и колонка, изменения которой ловит хеш.
RECONCILE_TABLES: dict[str, str | None] = {"orders": None, "payments": "status"}
PENDING_TABLE = "payments"


class Loader:
    def __init__(self, s: Settings, run_id: uuid.UUID | None = None):
        self.s = s
        # Под Airflow run_id общий для всех задач одного запуска DAG, из CLI свой на каждый прогон.
        self.run_id = run_id or uuid.uuid4()
        self.src = ch.mariadb(s)
        self.ch = ch.ch_loader(s)
        self.version = self._next_version()

    def _next_version(self) -> int:
        """Версия прогона: микросекунды сейчас, но не меньше уже записанной + 1.
        Так сдвиг часов сервера назад не сделает свежие строки «старше» прежних."""
        now_us = int(time.time() * 1_000_000)
        prev = 0
        for table in list(SMALL_TABLES) + list(BIG_TABLES):
            prev = max(prev, int(self._ch_scalar(f"SELECT max(_version) FROM raw.{table}") or 0))
        return max(now_us, prev + 1)

    # ---------- общие примитивы ----------

    def _src_rows(self, sql: str, params=None) -> list[tuple]:
        with self.src.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())

    def _src_rows_by_ids(self, table: str, cols: list[str], ids: list[int]) -> list[tuple]:
        rows: list[tuple] = []
        for i in range(0, len(ids), 1000):
            part = ids[i:i + 1000]
            rows += self._src_rows(
                f"SELECT {', '.join(cols)} FROM {table} WHERE id IN ({','.join(['%s'] * len(part))})", part)
        return rows

    def _insert(self, table: str, cols: list[str], rows: list[tuple], deleted: int = 0) -> int:
        if not rows:
            return 0
        data = [[as_utc(v) for v in r] + [self.version, deleted] for r in rows]
        self.ch.insert(f"raw.{table}", data, column_names=cols + ["_version", "_deleted"])
        return len(rows)

    def _log_run(self, table: str, strategy: str, started: datetime, status: str,
                 max_id: int, rows_read: int, rows_written: int, note: str = "") -> None:
        self.ch.insert(
            "meta.load_runs",
            [[self.run_id, table, strategy, started, utc_now(), status, max_id, rows_read, rows_written, note]],
            column_names=["run_id", "table_name", "strategy", "started_at", "finished_at", "status",
                          "max_id", "rows_read", "rows_written", "note"],
        )

    def _ch_scalar(self, sql: str):
        return self.ch.query(sql).result_rows[0][0]

    def _step(self, table: str, strategy: str, fn) -> None:
        """Выполняет шаг и пишет в meta.load_runs его итог, в том числе провал."""
        started = utc_now()
        try:
            max_id, rows_read, rows_written, note = fn()
        except Exception as e:
            self._log_run(table, strategy, started, "failed", 0, 0, 0, f"{type(e).__name__}: {e}"[:500])
            raise
        self._log_run(table, strategy, started, "ok", max_id, rows_read, rows_written, note)

    # ---------- стратегия 1: полная перезагрузка ----------

    def load_full(self, table: str, cols: list[str]) -> tuple:
        tmp = f"raw.{table}_new"
        # Пересоздаётся с нуля: уцелевшая после сбоя _new могла остаться со старой схемой.
        self.ch.command(f"DROP TABLE IF EXISTS {tmp}")
        self.ch.command(f"CREATE TABLE {tmp} AS raw.{table}")
        total, max_id = 0, 0
        with self.src.cursor() as cur:
            cur.execute(f"SELECT {', '.join(cols)} FROM {table} ORDER BY id")
            while rows := cur.fetchmany(self.s.load_chunk_rows):
                data = [[as_utc(v) for v in r] + [self.version, 0] for r in rows]
                self.ch.insert(tmp, data, column_names=cols + ["_version", "_deleted"])
                total, max_id = total + len(rows), rows[-1][0]
        # Подмена атомарна: читатели видят либо старую таблицу целиком, либо новую.
        self.ch.command(f"EXCHANGE TABLES raw.{table} AND {tmp}")
        self.ch.command(f"DROP TABLE {tmp}")
        log.info("%s: полная перезагрузка, %d строк", table, total)
        return max_id, total, total, ""

    # ---------- стратегия 2: инкремент по id ----------

    def load_incremental(self, table: str, cols: list[str]) -> tuple:
        # Водяной знак берётся из самой raw-таблицы: это то, что реально загружено,
        # поэтому упавший на середине прогон безопасен и не требует ручного отката.
        # Запас назад покрывает транзакции, зафиксированные не по порядку id;
        # повторно прочитанные строки схлопнет ReplacingMergeTree.
        max_loaded = int(self._ch_scalar(f"SELECT max(id) FROM raw.{table}") or 0)
        last_id = max(0, max_loaded - self.s.load_overlap_ids) if max_loaded else 0
        start_id, total = last_id, 0
        chunk = self.s.load_chunk_rows
        while True:
            rows = self._src_rows(
                f"SELECT {', '.join(cols)} FROM {table} WHERE id > %s ORDER BY id LIMIT %s", (last_id, chunk))
            total += self._insert(table, cols, rows)
            if rows:
                last_id = rows[-1][0]
            if len(rows) < chunk:
                break
        log.info("%s: инкремент, %d строк (id > %d)", table, total, start_id)
        return last_id, total, total, f"id > {start_id}"

    # ---------- часы источника и перечитка pending ----------

    def source_clock(self) -> datetime:
        """«Сейчас» по часам источника, а не по часам сервера: стенд имитирует сутки
        за минуты, а в проде реплика может отставать. Последняя строка по первичному
        ключу читается за одно обращение к индексу; у payments есть индекс по
        created_at, и там берётся максимум, потому что платежи приходят с опозданием."""
        marks = [r[0] for r in self._src_rows(
            "(SELECT created_at FROM orders ORDER BY id DESC LIMIT 1)"
            " UNION ALL (SELECT MAX(created_at) FROM payments)"
            " UNION ALL (SELECT activated_at FROM activations ORDER BY id DESC LIMIT 1)")]
        marks = [m for m in marks if m is not None]
        return max(marks) if marks else datetime(1970, 1, 1)

    def reread_pending(self) -> tuple:
        """Платежи, которые в raw ещё pending, перечитываются по id независимо от возраста."""
        table, cols = PENDING_TABLE, BIG_TABLES[PENDING_TABLE]
        ids = [r[0] for r in self.ch.query(f"SELECT id FROM stg.{table} WHERE status = 'pending'").result_rows]
        rows = self._src_rows_by_ids(table, cols, ids)
        written = self._insert(table, cols, rows)
        log.info("%s: перечитано pending %d", table, len(ids))
        return 0, len(ids), written, f"pending ids: {len(ids)}"

    # ---------- сверка по бакетам id ----------

    def reconcile(self, table: str, hash_col: str | None, now: datetime, full_hash: bool) -> tuple:
        cols = BIG_TABLES[table]
        b = self.s.reconcile_bucket
        # Хеш привязан к id, иначе обмен статусами двух строк внутри бакета даст ту же сумму.
        # Без хеша запрос идёт по вторичному индексу и не читает строки.
        hash_from = 0
        if hash_col and not full_hash:
            since = now - timedelta(days=self.s.reconcile_hash_days)
            hash_from = int(self._src_rows(
                f"SELECT COALESCE(MIN(id), 0) FROM {table} WHERE created_at >= %s", (since,))[0][0])
        # Два запроса вместо одного: число строк и сумма id идут по вторичному индексу без чтения
        # строк, а хеш статуса читает строки только в диапазоне id >= hash_from (диапазон по
        # первичному ключу). В одном запросе с IF() оптимизатор делает полный скан таблицы.
        src = {int(r[0]): [int(r[1]), int(r[2]), 0] for r in self._src_rows(
            f"SELECT id DIV {b}, COUNT(*), SUM(id) FROM {table} GROUP BY id DIV {b}")}
        dst = {int(r[0]): [int(r[1]), int(r[2]), 0] for r in self.ch.query(
            f"SELECT intDiv(id, {b}), count(), sum(id) FROM stg.{table} GROUP BY 1").result_rows}
        if hash_col:
            for r in self._src_rows(
                    f"SELECT id DIV {b}, SUM(CRC32(CONCAT(id, {hash_col}))) FROM {table} "
                    f"WHERE id >= %s GROUP BY id DIV {b}", (hash_from,)):
                src.setdefault(int(r[0]), [0, 0, 0])[2] = int(r[1])
            for r in self.ch.query(
                    f"SELECT intDiv(id, {b}), sum(CRC32(concat(toString(id), {hash_col}))) FROM stg.{table} "
                    f"WHERE id >= {hash_from} GROUP BY 1").result_rows:
                dst.setdefault(int(r[0]), [0, 0, 0])[2] = int(r[1])
        bad = sorted(k for k in set(src) | set(dst) if src.get(k) != dst.get(k))
        deleted, changed = [], []
        key_cols = f"id, {hash_col}" if hash_col else "id"
        for k in bad:
            lo, hi = k * b, (k + 1) * b
            s_rows = {r[0]: r for r in self._src_rows(
                f"SELECT {key_cols} FROM {table} WHERE id >= %s AND id < %s", (lo, hi))}
            d_rows = {r[0]: r for r in self.ch.query(
                f"SELECT {key_cols} FROM stg.{table} WHERE id >= {lo} AND id < {hi}").result_rows}
            deleted += [i for i in d_rows if i not in s_rows]
            changed += [i for i, r in s_rows.items() if d_rows.get(i) != r]
        # Маркер удаления = последняя известная версия строки с _deleted = 1.
        for i in range(0, len(deleted), 1000):
            ids = ",".join(str(x) for x in deleted[i:i + 1000])
            self.ch.command(
                f"INSERT INTO raw.{table} ({', '.join(cols)}, _version, _deleted) "
                f"SELECT {', '.join(cols)}, {self.version}, 1 FROM stg.{table} WHERE id IN ({ids})")
        rewritten = self._insert(table, cols, self._src_rows_by_ids(table, cols, changed))
        note = (f"buckets {len(src)}, mismatched {len(bad)}, deleted {len(deleted)}, changed {len(changed)}, "
                f"hash from id {hash_from}" + (" (full)" if full_hash else ""))
        log.info("%s: сверка%s, бакетов %d, расхождений %d, удалено %d, перечитано %d",
                 table, " полная" if full_hash else "", len(src), len(bad), len(deleted), len(changed))
        return 0, len(bad), len(deleted) + rewritten, note

    # ---------- весь шаг ----------

    def run(self) -> None:
        for table, cols in SMALL_TABLES.items():
            self._step(table, "full", lambda t=table, c=cols: self.load_full(t, c))
        for table, cols in BIG_TABLES.items():
            self._step(table, "incremental", lambda t=table, c=cols: self.load_incremental(t, c))
        self._step(PENDING_TABLE, "pending", self.reread_pending)
        now = self.source_clock()
        for table, hash_col in RECONCILE_TABLES.items():
            self._step(table, "reconcile", lambda t=table, h=hash_col: self.reconcile(t, h, now, full_hash=False))

    def audit(self) -> None:
        """Полная сверка с хешем по всей таблице: отдельная команда, в проде раз в сутки."""
        now = self.source_clock()
        for table, hash_col in RECONCILE_TABLES.items():
            self._step(table, "audit", lambda t=table, h=hash_col: self.reconcile(t, h, now, full_hash=True))


# ---------- точки входа: CLI вызывает load_all/audit_all, задачи Airflow вызывают по шагу ----------

def load_table(s: Settings, table: str, run_id: uuid.UUID | None = None) -> None:
    """Загрузка одной таблицы её стратегией: полная перезагрузка или инкремент по id."""
    loader = Loader(s, run_id)
    if table in SMALL_TABLES:
        loader._step(table, "full", lambda: loader.load_full(table, SMALL_TABLES[table]))
    else:
        loader._step(table, "incremental", lambda: loader.load_incremental(table, BIG_TABLES[table]))


def reread_pending(s: Settings, run_id: uuid.UUID | None = None) -> None:
    loader = Loader(s, run_id)
    loader._step(PENDING_TABLE, "pending", loader.reread_pending)


def reconcile_table(s: Settings, table: str, run_id: uuid.UUID | None = None, full: bool = False) -> None:
    """Сверка по бакетам id; full=True это audit: хеш по всей таблице, в проде раз в сутки."""
    loader = Loader(s, run_id)
    now = loader.source_clock()
    loader._step(table, "audit" if full else "reconcile",
                 lambda: loader.reconcile(table, RECONCILE_TABLES[table], now, full_hash=full))


def load_all(s: Settings, run_id: uuid.UUID | None = None) -> None:
    Loader(s, run_id).run()


def audit_all(s: Settings) -> None:
    Loader(s).audit()
