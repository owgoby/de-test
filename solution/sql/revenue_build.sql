-- Сборка витрины mart.revenue в mart.revenue_new. Подмену (EXCHANGE TABLES) делает
-- pipeline/mart.py после блокирующих проверок новой таблицы.
-- Полная пересборка выбрана потому, что поля истории пользователя (first_paid_at, когорта,
-- purchase_number) меняются у старых строк при любом новом или отозванном платеже этого
-- пользователя, а объём витрины (десятки тысяч строк, при 100-кратном росте миллионы)
-- ClickHouse пересчитывает за секунды. Запросы выполняются по порядку, каждый отдельно.

DROP TABLE IF EXISTS mart.revenue_new;

CREATE TABLE mart.revenue_new AS mart.revenue;

INSERT INTO mart.revenue_new
WITH
    -- Заказы, которые попадают в витрину: не тестовый и не сотрудник по текущему состоянию
    -- users; оплачен баллами (payment_uid пустой) или платёж есть и в статусе success.
    eligible AS
    (
        SELECT
            o.id                                   AS order_id,
            o.user_id                              AS user_id,
            ifNull(o.payment_uid, '')              AS payment_uid,
            if(payment_uid = '', 'points', 'payment') AS source,
            if(source = 'payment', p.created_at, o.created_at) AS paid_at,
            o.plan_id                              AS plan_id,
            u.country                              AS user_country,
            multiIf(
                pl.is_unlimited = 1 AND pl.days < 7, 'unlim_short',
                pl.is_unlimited = 1 AND pl.days = 7, 'unlim_mid',
                pl.is_unlimited = 1,                 'unlim_long',
                assumeNotNull(pl.size_gb) <= 1,      'small',
                assumeNotNull(pl.size_gb) <= 10,     'mid',
                                                     'big') AS plan_category,
            if(source = 'payment', p.amount_eur, toDecimal64(0, 2)) AS revenue_eur
        FROM stg.orders AS o
        INNER JOIN stg.users AS u ON u.id = o.user_id
        LEFT JOIN stg.plans AS pl ON pl.id = o.plan_id   -- заказ без плана не теряется молча: его ловит проверка
        LEFT JOIN stg.payments AS p ON p.payment_uid = o.payment_uid
        WHERE u.is_tester = 0 AND u.is_staff = 0
          AND (source = 'points' OR p.status = 'success')
    ),
    refunds AS
    (
        SELECT payment_uid, sum(amount_eur) AS refund_eur
        FROM stg.refunds
        GROUP BY payment_uid
    ),
    -- Себестоимость: usage_daily -> activations -> order_id
    cogs AS
    (
        SELECT a.order_id AS order_id, sum(ud.cost_eur) AS cogs_eur
        FROM stg.usage_daily AS ud
        INNER JOIN stg.activations AS a ON a.id = ud.activation_id
        GROUP BY a.order_id
    ),
    money AS
    (
        SELECT
            -- Явные алиасы обязательны: без них анализатор ClickHouse оставляет имена e.order_id.
            e.order_id      AS order_id,
            e.user_id       AS user_id,
            e.payment_uid   AS payment_uid,
            e.source        AS source,
            e.paid_at       AS paid_at,
            e.plan_id       AS plan_id,
            e.plan_category AS plan_category,
            e.user_country  AS user_country,
            e.revenue_eur   AS revenue_eur,
            toDecimal64(ifNull(r.refund_eur, 0), 2) AS refund_eur,
            toDecimal64(ifNull(c.cogs_eur, 0), 2)   AS cogs_eur
        FROM eligible AS e
        -- Возвраты только у заказов за деньги: пустой payment_uid заказа за баллы
        -- не должен совпасть с возвратом, у которого payment_uid пустой по ошибке.
        LEFT JOIN refunds AS r ON r.payment_uid = e.payment_uid AND e.source = 'payment'
        LEFT JOIN cogs    AS c ON c.order_id = e.order_id
    )
SELECT
    order_id,
    user_id,
    payment_uid,
    source,
    paid_at,
    toDate(paid_at)                                   AS pdate,
    plan_id,
    plan_category,
    user_country,
    revenue_eur,
    refund_eur,
    cogs_eur,
    revenue_eur - cogs_eur                            AS gross_profit_eur,
    -- Самый ранний платёж деньгами пользователя; заполняется и в строках points.
    minIf(toNullable(paid_at), source = 'payment') OVER (PARTITION BY user_id) AS first_paid_at,
    toStartOfMonth(first_paid_at)                     AS first_cohort,
    if(source = 'payment',
       (toYear(paid_at) - toYear(assumeNotNull(first_paid_at))) * 12
         + toMonth(paid_at) - toMonth(assumeNotNull(first_paid_at)) + 1,
       0)                                             AS month_since_first,
    if(source = 'payment',
       toNullable(toUInt32(row_number() OVER (PARTITION BY user_id, source ORDER BY paid_at, order_id))),
       NULL)                                          AS purchase_number
FROM money;
