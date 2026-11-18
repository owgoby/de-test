"""Свойства источника из ТЗ, которых нет в шести пакетах стенда, и идемпотентность.

Тесты идут по порядку и продолжают друг друга, как прогоны в течение суток: каждый шаг меняет
источник, запускает пайплайн и сверяет витрину с эталоном на pandas, посчитанным прямо из MariaDB.
"""

RECONCILE_MISMATCHES_LAST_RUN = """
    SELECT countIf(note NOT LIKE '%mismatched 0,%') FROM meta.load_runs
    WHERE strategy = 'reconcile' AND run_id = (SELECT argMax(run_id, started_at) FROM meta.load_runs)
"""
MART_FINGERPRINT = "SELECT tuple(count(), sum(cityHash64(*))) FROM mart.revenue"

# Последний успешный платёж обычного пользователя с заказом: его отзывает sql/step1.sql.
RECENT_SUCCESS_PAYMENT = """
    SELECT p.payment_uid FROM payments p
    JOIN users u ON u.id = p.user_id JOIN orders o ON o.payment_uid = p.payment_uid
    WHERE p.status = 'success' AND u.is_tester = 0 AND u.is_staff = 0
    ORDER BY p.id DESC LIMIT 1
"""
# Первый платёж пользователя с тремя и более успешными оплатами: его отзывает sql/step3.sql.
OLD_SUCCESS_PAYMENT = """
    SELECT p.payment_uid FROM payments p JOIN users u ON u.id = p.user_id
    WHERE p.status = 'success' AND u.is_tester = 0 AND u.is_staff = 0
      AND (SELECT COUNT(*) FROM payments p2 WHERE p2.user_id = p.user_id AND p2.status = 'success') >= 3
    ORDER BY p.id LIMIT 1
"""


def test_initial_load_matches_reference(stand):
    stand.pipeline()
    stand.verify()


def test_second_run_without_changes_is_idempotent(stand):
    before = stand.ch_value(MART_FINGERPRINT)
    stand.pipeline()
    assert stand.ch_value(MART_FINGERPRINT) == before, "витрина изменилась, хотя источник не менялся"
    assert stand.ch_value(RECONCILE_MISMATCHES_LAST_RUN) == 0, "сверка нашла расхождения при неизменном источнике"
    stand.verify()


def test_flags_plan_change_recall_and_late_payment(stand):
    """Снят тестовый флаг; пользователь стал сотрудником; план сменил категорию;
    свежий платёж success -> declined; опоздавший на 12 дней pending-платёж с заказом."""
    recalled = stand.src_value(RECENT_SUCCESS_PAYMENT)
    stand.apply("step1.sql")
    stand.pipeline()
    stand.verify()
    assert stand.mart_rows_with_payment(recalled) == 0, "отзыв свежего платежа не пойман сверкой в штатном прогоне"


def test_late_payment_confirmed(stand):
    stand.apply("step2.sql")
    stand.pipeline()
    stand.verify()
    assert stand.ch_value("SELECT count() FROM mart.revenue WHERE payment_uid LIKE 'eeeeeeee-%'") == 1


def test_old_payment_recall_found_by_audit(stand):
    """Отзыв платежа старше окна хеша (30 дней): штатный прогон его не видит, это ожидаемо;
    audit находит, следующий прогон убирает строку и пересчитывает поля истории пользователя."""
    recalled = stand.src_value(OLD_SUCCESS_PAYMENT)
    stand.apply("step3.sql")

    stand.pipeline()
    assert stand.mart_rows_with_payment(recalled) == 1, "штатный прогон не должен видеть старый платёж"

    stand.pipeline("audit")
    stand.pipeline()
    assert stand.mart_rows_with_payment(recalled) == 0, "строка отозванного платежа осталась после audit"
    stand.verify()
