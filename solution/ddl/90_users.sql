-- Пользователи и права. Пароли подставляются из окружения, в файле их нет.
-- CREATE IF NOT EXISTS + ALTER: повторное применение не падает и подхватывает смену пароля.

CREATE USER IF NOT EXISTS {{CLICKHOUSE_LOADER_USER}} IDENTIFIED WITH sha256_password BY '{{CLICKHOUSE_LOADER_PASSWORD}}';
ALTER USER {{CLICKHOUSE_LOADER_USER}} IDENTIFIED WITH sha256_password BY '{{CLICKHOUSE_LOADER_PASSWORD}}';

CREATE USER IF NOT EXISTS {{CLICKHOUSE_ANALYST_USER}} IDENTIFIED WITH sha256_password BY '{{CLICKHOUSE_ANALYST_PASSWORD}}';
ALTER USER {{CLICKHOUSE_ANALYST_USER}} IDENTIFIED WITH sha256_password BY '{{CLICKHOUSE_ANALYST_PASSWORD}}';

-- loader: пишет во все слои (загрузка, пересборка витрины, служебные таблицы), DDL не делает.
GRANT SELECT, INSERT, ALTER, TRUNCATE, CREATE TABLE, DROP TABLE ON meta.* TO {{CLICKHOUSE_LOADER_USER}};
GRANT SELECT, INSERT, ALTER, TRUNCATE, CREATE TABLE, DROP TABLE ON raw.*  TO {{CLICKHOUSE_LOADER_USER}};
GRANT SELECT ON stg.* TO {{CLICKHOUSE_LOADER_USER}};
GRANT SELECT, INSERT, ALTER, TRUNCATE, CREATE TABLE, DROP TABLE ON mart.* TO {{CLICKHOUSE_LOADER_USER}};

-- analyst: только чтение опубликованной витрины mart.revenue.
-- Право на таблицу, а не на mart.*: иначе аналитик видел бы mart.revenue_new, пока она собирается
-- и ещё не прошла блокирующие проверки. Право выдаётся по имени, поэтому после EXCHANGE TABLES
-- оно действует на новую версию витрины.
-- WITH REPLACE OPTION атомарно заменяет все права analyst на эти: повторное применение DDL снимает
-- выданное раньше (mart.*, stg.*), и нет момента, когда прав нет совсем.
-- Права на новые витрины выдаются отдельными GRANT ниже этой строки, без REPLACE OPTION.
GRANT SELECT ON mart.revenue TO {{CLICKHOUSE_ANALYST_USER}} WITH REPLACE OPTION;

-- Профиль analyst: лимиты памяти и времени запроса, только чтение.
-- CONST запрещает менять лимит в запросе: без него SETTINGS max_memory_usage = 0 снимал бы лимит.
-- MAX не подходит: 0 у этих настроек значит «без лимита» и под ограничение MAX проходит.
-- Профиль создаётся один раз и дальше меняется ALTER: CREATE OR REPLACE пересоздавал бы его
-- с новым id и до ALTER USER отвязывал от пользователя, то есть снимал лимиты и readonly.
CREATE SETTINGS PROFILE IF NOT EXISTS analyst_profile;
ALTER SETTINGS PROFILE analyst_profile
    SETTINGS max_memory_usage = 4000000000 CONST, max_execution_time = 300 CONST, readonly = 2;
ALTER USER {{CLICKHOUSE_ANALYST_USER}} SETTINGS PROFILE 'analyst_profile';
