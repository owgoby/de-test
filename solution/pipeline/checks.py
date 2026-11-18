"""Проверки: инварианты в SQL (каждый прогон) и сверка с источником плюс эталон на pandas
(команда verify, только для стенда: читает источник целиком).

Инварианты отвечают на вопрос «не противоречит ли витрина сама себе и источнику»:
уникальность, отсутствие тестовых пользователей, согласованность сумм, совпадение
числа строк в stg и в MariaDB. Эталон отвечает на вопрос «правильно ли посчитано»:
витрина считается второй раз, другим способом (pandas) и из другого места (прямо из
MariaDB, минуя raw и stg), и сравнивается с mart.revenue поколоночно. Полное чтение
источника допустимо только на стенде; в проде эталон считается по выборке пользователей.

Инварианты делятся на две группы. MART_INVARIANTS читают только саму витрину и выполняются
и до публикации, и после неё. STAGE_INVARIANTS сверяют витрину со stg и выполняются только
до публикации (mart.py), в том же шаге и под тем же pool, что и сборка: после публикации stg
может измениться (ночной audit пишет в raw), и такая проверка упала бы на верной витрине.

Результаты пишутся в meta.check_results: блокирующие проверки до публикации с префиксом
«до публикации: » (mart.py), остальные здесь. Любой провал завершает прогон ненулевым кодом.
"""
import logging
import uuid

import pandas as pd

from pipeline import ch
from pipeline.config import Settings, utc_now
from pipeline.load import BIG_TABLES, SMALL_TABLES

log = logging.getLogger(__name__)

# Имя проверки -> SQL, который возвращает одно число нарушений (0 = прошла).
# {t} — проверяемая таблица: mart.revenue_new до публикации (mart.py) и mart.revenue после неё.

# Инварианты самой витрины: читают только {t}.
MART_INVARIANTS: dict[str, str] = {
    "order_id уникален":
        "SELECT count() - uniqExact(order_id) FROM {t}",
    "у points-строк нулевая выручка и пустой purchase_number":
        "SELECT count() FROM {t} WHERE source = 'points' "
        "AND (revenue_eur != 0 OR month_since_first != 0 OR purchase_number IS NOT NULL)",
    "gross_profit = revenue - cogs":
        "SELECT count() FROM {t} WHERE gross_profit_eur != revenue_eur - cogs_eur",
    "purchase_number нумерует платежи пользователя без пропусков":
        "SELECT count() FROM (SELECT user_id, countIf(source = 'payment') AS n, "
        "max(purchase_number) AS mx, uniqExact(purchase_number) AS u FROM {t} GROUP BY user_id) "
        "WHERE n > 0 AND (mx != n OR u != n)",
    "first_paid_at одинаков во всех строках пользователя и равен min(paid_at) платежей":
        "SELECT count() FROM (SELECT user_id, countIf(source = 'payment') AS n, "
        "uniqExact(first_paid_at) AS u, countIf(first_paid_at IS NULL) AS nulls, "
        "minIf(paid_at, source = 'payment') AS mn, any(first_paid_at) AS f "
        "FROM {t} GROUP BY user_id) "
        "WHERE (n > 0 AND (u != 1 OR nulls > 0 OR f != mn)) OR (n = 0 AND u != 0)",
}

# Согласованность витрины со stg: только до публикации, см. docstring модуля.
STAGE_INVARIANTS: dict[str, str] = {
    "нет тестовых и сотрудников":
        "SELECT count() FROM {t} r INNER JOIN stg.users u ON u.id = r.user_id "
        "WHERE u.is_tester = 1 OR u.is_staff = 1",
    "у payment-строк платёж success":
        "SELECT count() FROM {t} r LEFT JOIN stg.payments p ON p.payment_uid = r.payment_uid "
        "WHERE r.source = 'payment' AND p.status != 'success'",
    "каждый подходящий заказ попал в витрину ровно один раз":
        "SELECT abs(count() - (SELECT count() FROM {t})) FROM stg.orders o "
        "INNER JOIN stg.users u ON u.id = o.user_id "
        "LEFT JOIN stg.payments p ON p.payment_uid = o.payment_uid "
        "WHERE u.is_tester = 0 AND u.is_staff = 0 AND (ifNull(o.payment_uid, '') = '' OR p.status = 'success')",
}


def record_results(client, run_id: uuid.UUID, results: list[tuple[str, bool, object, object]]) -> None:
    """Пишет результаты проверок (имя, прошла ли, измерено, ожидалось) в meta.check_results и в лог."""
    now = utc_now()
    client.insert(
        "meta.check_results",
        [[run_id, now, name, int(passed), str(measured), str(expected)]
         for name, passed, measured, expected in results],
        column_names=["run_id", "checked_at", "check_name", "passed", "measured", "expected"],
    )
    for name, passed, measured, expected in results:
        (log.info if passed else log.error)("%s: %s (измерено %s, ожидалось %s)",
                                             "OK " if passed else "FAIL", name, measured, expected)


class Checker:
    def __init__(self, s: Settings, run_id: uuid.UUID | None = None):
        self.s = s
        self.run_id = run_id or uuid.uuid4()
        self.ch = ch.ch_loader(s)
        self.src = ch.mariadb(s)
        self.failed: list[str] = []

    def _record(self, name: str, passed: bool, measured, expected) -> None:
        record_results(self.ch, self.run_id, [(name, passed, measured, expected)])
        if not passed:
            self.failed.append(name)

    # ---------- 1. инварианты ----------

    def invariants(self) -> None:
        """После публикации: только инварианты самой витрины, stg не читается."""
        for name, sql in MART_INVARIANTS.items():
            violations = self.ch.query(sql.replace("{t}", "mart.revenue")).result_rows[0][0]
            self._record(name, violations == 0, violations, 0)

    # ---------- 2. stg против источника ----------

    def source_counts(self) -> None:
        with self.src.cursor() as cur:
            for table in list(SMALL_TABLES) + list(BIG_TABLES):
                cur.execute(f"SELECT COUNT(*), COALESCE(MAX(id), 0) FROM {table}")
                src_n, src_max = cur.fetchone()
                dst_n, dst_max = self.ch.query(f"SELECT count(), max(id) FROM stg.{table}").result_rows[0]
                self._record(f"stg.{table}: число строк и max(id) как в источнике",
                             (src_n, src_max) == (dst_n, dst_max), (dst_n, dst_max), (src_n, src_max))
            cur.execute("SELECT COALESCE(SUM(amount_eur), 0) FROM payments WHERE status = 'success'")
            src_sum = cur.fetchone()[0]
        dst_sum = self.ch.query("SELECT sum(amount_eur) FROM stg.payments WHERE status = 'success'").result_rows[0][0]
        self._record("stg.payments: сумма success-платежей как в источнике", src_sum == dst_sum, dst_sum, src_sum)

    # ---------- 3. эталон на pandas из MariaDB ----------

    def _df(self, sql: str) -> pd.DataFrame:
        with self.src.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)

    def reference(self) -> pd.DataFrame:
        users = self._df("SELECT id AS user_id, country, is_tester, is_staff FROM users")
        plans = self._df("SELECT id AS plan_id, size_gb, days, is_unlimited FROM plans")
        payments = self._df("SELECT payment_uid, status, amount_eur, created_at AS p_created_at FROM payments")
        orders = self._df("SELECT id AS order_id, user_id, payment_uid, plan_id, created_at AS o_created_at FROM orders")
        activations = self._df("SELECT id AS activation_id, order_id FROM activations")
        usage = self._df("SELECT activation_id, cost_eur FROM usage_daily")
        refunds = self._df("SELECT payment_uid, amount_eur AS refund_eur FROM refunds")

        df = orders.merge(users[(users.is_tester == 0) & (users.is_staff == 0)], on="user_id")
        df["payment_uid"] = df["payment_uid"].fillna("")
        df["source"] = (df["payment_uid"] == "").map({True: "points", False: "payment"})
        df = df.merge(payments, on="payment_uid", how="left")
        df = df[(df["source"] == "points") | (df["status"] == "success")].copy()
        df["paid_at"] = df["p_created_at"].where(df["source"] == "payment", df["o_created_at"])
        df = df.merge(plans, on="plan_id")

        def category(r):
            if r.is_unlimited == 1:
                return "unlim_short" if r.days < 7 else ("unlim_mid" if r.days == 7 else "unlim_long")
            size = float(r.size_gb)
            return "small" if size <= 1 else ("mid" if size <= 10 else "big")

        df["plan_category"] = df.apply(category, axis=1)
        df["revenue_eur"] = df["amount_eur"].where(df["source"] == "payment", 0).astype(float).round(2)
        ref = refunds.groupby("payment_uid", as_index=False)["refund_eur"].sum()
        df = df.merge(ref, on="payment_uid", how="left")
        cogs = usage.merge(activations, on="activation_id").groupby("order_id", as_index=False)["cost_eur"].sum()
        cogs = cogs.rename(columns={"cost_eur": "cogs_eur"})
        df = df.merge(cogs, on="order_id", how="left")
        df["refund_eur"] = df["refund_eur"].fillna(0).astype(float).round(2)
        df.loc[df["source"] == "points", "refund_eur"] = 0.0  # возвраты только у заказов за деньги
        df["cogs_eur"] = df["cogs_eur"].fillna(0).astype(float).round(2)
        df["gross_profit_eur"] = (df["revenue_eur"] - df["cogs_eur"]).round(2)

        pay = df[df["source"] == "payment"]
        first = pay.groupby("user_id")["paid_at"].min().rename("first_paid_at")
        df = df.merge(first, on="user_id", how="left")
        df["first_cohort"] = df["first_paid_at"].dt.to_period("M").dt.start_time.dt.date
        is_pay = df["source"] == "payment"
        months = (df["paid_at"].dt.year - df["first_paid_at"].dt.year) * 12 \
            + (df["paid_at"].dt.month - df["first_paid_at"].dt.month) + 1
        df["month_since_first"] = months.where(is_pay, 0).astype(int)
        df = df.sort_values(["user_id", "paid_at", "order_id"])
        is_pay = df["source"] == "payment"
        df["purchase_number"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        df.loc[is_pay, "purchase_number"] = (df[is_pay].groupby("user_id").cumcount() + 1).astype("Float64")
        df["pdate"] = df["paid_at"].dt.date
        cols = ["order_id", "user_id", "payment_uid", "source", "paid_at", "pdate", "plan_id", "plan_category",
                "country", "revenue_eur", "refund_eur", "cogs_eur", "gross_profit_eur", "first_paid_at",
                "first_cohort", "month_since_first", "purchase_number"]
        return df[cols].rename(columns={"country": "user_country"}).sort_values("order_id").reset_index(drop=True)

    def compare_with_reference(self) -> None:
        expected = self.reference()
        actual = self.ch.query_df("SELECT * FROM mart.revenue ORDER BY order_id")
        self._record("эталон: число строк", len(actual) == len(expected), len(actual), len(expected))
        same_ids = set(actual["order_id"]) == set(expected["order_id"])
        self._record("эталон: набор order_id", same_ids,
                     len(set(actual["order_id"]) ^ set(expected["order_id"])), 0)
        if not same_ids:
            return
        a = actual.set_index("order_id").sort_index()
        e = expected.set_index("order_id").sort_index()
        for col in e.columns:
            av, ev = a[col], e[col]
            if col in ("paid_at", "first_paid_at"):
                av = pd.to_datetime(av).dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")
                ev = pd.to_datetime(ev).dt.strftime("%Y-%m-%d %H:%M:%S")
            elif col in ("pdate", "first_cohort"):
                av, ev = av.astype(str), ev.astype(str)
            elif col in ("revenue_eur", "refund_eur", "cogs_eur", "gross_profit_eur"):
                av, ev = av.astype(float).round(2), ev.astype(float).round(2)
            elif col == "purchase_number":
                av, ev = av.astype("float"), ev.astype("float")
            else:
                av, ev = av.astype(str), ev.astype(str)
            av, ev = av.fillna("∅").astype(str), ev.fillna("∅").astype(str)
            diff = (av != ev).sum()
            sample = "" if diff == 0 else f"; пример order_id={av[av != ev].index[0]}: {av[av != ev].iloc[0]} vs {ev[av != ev].iloc[0]}"
            self._record(f"эталон: колонка {col}", diff == 0, f"{diff} расхождений{sample}", 0)

    def _finish(self) -> bool:
        if self.failed:
            log.error("проверок провалено: %d", len(self.failed))
        else:
            log.info("все проверки пройдены")
        return not self.failed

    def run(self) -> bool:
        """Каждый прогон после публикации: только инварианты самой витрины, ни stg, ни источник не читаются."""
        self.invariants()
        return self._finish()

    def verify(self) -> bool:
        """Стенд: сверка с источником и эталон. Читает большие таблицы целиком."""
        self.source_counts()
        self.compare_with_reference()
        return self._finish()


def run_checks(s: Settings, run_id: uuid.UUID | None = None) -> bool:
    return Checker(s, run_id).run()


def verify_against_source(s: Settings) -> bool:
    return Checker(s).verify()
