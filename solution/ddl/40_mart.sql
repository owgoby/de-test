-- Витрина revenue: одна строка на оплаченный заказ. Колонки и порядок строго по ТЗ.
-- Пересобирается целиком в mart.revenue_new и подменяется EXCHANGE TABLES,
-- поэтому аналитики никогда не видят полупустую таблицу.
CREATE TABLE IF NOT EXISTS mart.revenue
(
    order_id          UInt64,
    user_id           UInt32,
    payment_uid       String,                      -- пусто для заказов за баллы
    source            LowCardinality(String),      -- payment | points
    paid_at           DateTime('UTC'),
    pdate             Date,
    plan_id           UInt32,
    plan_category     LowCardinality(String),
    user_country      LowCardinality(String),
    revenue_eur       Decimal(10, 2),
    refund_eur        Decimal(10, 2),
    cogs_eur          Decimal(10, 2),
    gross_profit_eur  Decimal(10, 2),
    first_paid_at     Nullable(DateTime('UTC')),
    first_cohort      Nullable(Date),
    month_since_first Int32,
    purchase_number   Nullable(UInt32)
)
ENGINE = MergeTree
ORDER BY order_id;
