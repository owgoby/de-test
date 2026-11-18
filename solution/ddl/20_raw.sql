-- Слой raw: копии таблиц источника на ReplacingMergeTree.
-- Служебные колонки одинаковы для всех таблиц:
--   _version   версия строки (время загрузки в микросекундах); при схлопывании побеждает большая;
--   _deleted   1 = строка удалена в источнике (маркер удаления, новая версия той же строки);
--   _loaded_at когда строка записана в ClickHouse.
-- ORDER BY id: ключ источника, по нему схлопываются версии.
-- Партиции по месяцу неизменяемой даты только у больших таблиц; маленькие без партиций.

CREATE TABLE IF NOT EXISTS raw.users
(
    id         UInt32,
    country    LowCardinality(String),
    is_tester  UInt8,
    is_staff   UInt8,
    created_at DateTime('UTC'),
    _version   UInt64,
    _deleted   UInt8 DEFAULT 0,
    _loaded_at DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY id;

CREATE TABLE IF NOT EXISTS raw.plans
(
    id           UInt32,
    name         String,
    size_gb      Nullable(Decimal(6, 1)),
    days         Int32,
    is_unlimited UInt8,
    price_eur    Decimal(10, 2),
    _version     UInt64,
    _deleted     UInt8 DEFAULT 0,
    _loaded_at   DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY id;

CREATE TABLE IF NOT EXISTS raw.payment_systems
(
    id         UInt32,
    name       String,
    _version   UInt64,
    _deleted   UInt8 DEFAULT 0,
    _loaded_at DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY id;

CREATE TABLE IF NOT EXISTS raw.payments
(
    id                UInt64,
    payment_uid       String,
    user_id           UInt32,
    payment_system_id UInt32,
    status            LowCardinality(String),
    amount_eur        Decimal(10, 2),
    created_at        DateTime('UTC'),
    _version          UInt64,
    _deleted          UInt8 DEFAULT 0,
    _loaded_at        DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version, _deleted)
PARTITION BY toYYYYMM(created_at)
ORDER BY id;

CREATE TABLE IF NOT EXISTS raw.orders
(
    id          UInt64,
    user_id     UInt32,
    payment_uid Nullable(String),
    plan_id     UInt32,
    points_used Int32,
    platform    LowCardinality(String),
    created_at  DateTime('UTC'),
    _version    UInt64,
    _deleted    UInt8 DEFAULT 0,
    _loaded_at  DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version, _deleted)
PARTITION BY toYYYYMM(created_at)
ORDER BY id;

CREATE TABLE IF NOT EXISTS raw.activations
(
    id           UInt64,
    order_id     UInt64,
    user_id      UInt32,
    activated_at DateTime('UTC'),
    _version     UInt64,
    _deleted     UInt8 DEFAULT 0,
    _loaded_at   DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version, _deleted)
PARTITION BY toYYYYMM(activated_at)
ORDER BY id;

CREATE TABLE IF NOT EXISTS raw.usage_daily
(
    id            UInt64,
    activation_id UInt64,
    usage_date    Date,
    usage_units   Int32,
    cost_eur      Decimal(10, 2),
    _version      UInt64,
    _deleted      UInt8 DEFAULT 0,
    _loaded_at    DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version, _deleted)
PARTITION BY toYYYYMM(usage_date)
ORDER BY id;

CREATE TABLE IF NOT EXISTS raw.refunds
(
    id          UInt64,
    payment_uid String,
    amount_eur  Decimal(10, 2),
    created_at  DateTime('UTC'),
    _version    UInt64,
    _deleted    UInt8 DEFAULT 0,
    _loaded_at  DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version, _deleted)
PARTITION BY toYYYYMM(created_at)
ORDER BY id;
