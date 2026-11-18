-- Слой stg: одна актуальная версия каждой строки, без удалённых, без служебных колонок.
-- FINAL схлопывает версии на чтении; WHERE _deleted = 0 убирает маркеры удаления.
-- На стенде это представления; в проде для крупных таблиц их материализует dbt.

CREATE OR REPLACE VIEW stg.users AS
SELECT id, country, is_tester, is_staff, created_at
FROM raw.users FINAL WHERE _deleted = 0;

CREATE OR REPLACE VIEW stg.plans AS
SELECT id, name, size_gb, days, is_unlimited, price_eur
FROM raw.plans FINAL WHERE _deleted = 0;

CREATE OR REPLACE VIEW stg.payment_systems AS
SELECT id, name
FROM raw.payment_systems FINAL WHERE _deleted = 0;

CREATE OR REPLACE VIEW stg.payments AS
SELECT id, payment_uid, user_id, payment_system_id, status, amount_eur, created_at
FROM raw.payments FINAL WHERE _deleted = 0;

CREATE OR REPLACE VIEW stg.orders AS
SELECT id, user_id, payment_uid, plan_id, points_used, platform, created_at
FROM raw.orders FINAL WHERE _deleted = 0;

CREATE OR REPLACE VIEW stg.activations AS
SELECT id, order_id, user_id, activated_at
FROM raw.activations FINAL WHERE _deleted = 0;

CREATE OR REPLACE VIEW stg.usage_daily AS
SELECT id, activation_id, usage_date, usage_units, cost_eur
FROM raw.usage_daily FINAL WHERE _deleted = 0;

CREATE OR REPLACE VIEW stg.refunds AS
SELECT id, payment_uid, amount_eur, created_at
FROM raw.refunds FINAL WHERE _deleted = 0;
