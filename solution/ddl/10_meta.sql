-- История загрузки по таблицам: каждый шаг каждого прогона, в том числе упавший.
CREATE TABLE IF NOT EXISTS meta.load_runs
(
    run_id       UUID,
    table_name   LowCardinality(String),
    strategy     LowCardinality(String),   -- full | incremental | pending | reconcile | audit
    started_at   DateTime('UTC'),
    finished_at  Nullable(DateTime('UTC')),
    status       LowCardinality(String),   -- ok | failed
    max_id       UInt64,                   -- максимальный id источника после шага
    rows_read    UInt64,
    rows_written UInt64,
    note         String
)
ENGINE = MergeTree
ORDER BY (table_name, started_at);

-- Результаты проверок витрины и сверки с источником: что проверено, прошло ли, какое значение измерено.
CREATE TABLE IF NOT EXISTS meta.check_results
(
    run_id     UUID,
    checked_at DateTime('UTC'),
    check_name LowCardinality(String),
    passed     UInt8,
    measured   String,
    expected   String
)
ENGINE = MergeTree
ORDER BY (checked_at, check_name);
