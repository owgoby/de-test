"""Настройки из переменных окружения. В коде нет ни одного пароля."""
import os
from dataclasses import dataclass
from datetime import UTC, datetime


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise RuntimeError(f"не задана переменная окружения {name}")
    return value


def utc_now() -> datetime:
    """Текущее время UTC с явным часовым поясом. clickhouse-connect переводит datetime в число
    через timestamp(), и время без пояса посчитал бы местным временем процесса: при TZ, отличном
    от UTC, отметки в meta сдвинулись бы на разницу поясов."""
    return datetime.now(UTC)


def as_utc(value):
    """Datetime из MariaDB приходит без часового пояса; все даты источника в UTC, поэтому пояс
    проставляется явно. Так вставка в ClickHouse и параметры запросов не зависят от TZ процесса.
    Остальные значения (и date) возвращаются как есть."""
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


@dataclass(frozen=True)
class Settings:
    mariadb_host: str
    mariadb_port: int
    mariadb_db: str
    mariadb_user: str
    mariadb_password: str
    ch_host: str
    ch_port: int
    ch_admin_user: str
    ch_admin_password: str
    ch_loader_user: str
    ch_loader_password: str
    ch_analyst_user: str
    ch_analyst_password: str
    results_dir: str
    load_chunk_rows: int
    load_overlap_ids: int
    reconcile_hash_days: int
    reconcile_bucket: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            mariadb_host=_env("MARIADB_HOST", "localhost"),
            mariadb_port=int(_env("MARIADB_PORT", "3306")),
            mariadb_db=_env("MARIADB_DB", "app"),
            mariadb_user=_env("MARIADB_USER", "root"),
            mariadb_password=_env("MARIADB_PASSWORD", os.environ.get("MARIADB_ROOT_PASSWORD")),
            ch_host=_env("CLICKHOUSE_HOST", "localhost"),
            ch_port=int(_env("CLICKHOUSE_PORT", "8123")),
            ch_admin_user=_env("CLICKHOUSE_ADMIN_USER"),
            ch_admin_password=_env("CLICKHOUSE_ADMIN_PASSWORD"),
            ch_loader_user=_env("CLICKHOUSE_LOADER_USER"),
            ch_loader_password=_env("CLICKHOUSE_LOADER_PASSWORD"),
            ch_analyst_user=_env("CLICKHOUSE_ANALYST_USER"),
            ch_analyst_password=_env("CLICKHOUSE_ANALYST_PASSWORD"),
            results_dir=_env("RESULTS_DIR", "/results"),
            load_chunk_rows=int(_env("LOAD_CHUNK_ROWS", "50000")),
            load_overlap_ids=int(_env("LOAD_OVERLAP_IDS", "1000")),
            reconcile_hash_days=int(_env("RECONCILE_HASH_DAYS", "30")),
            reconcile_bucket=int(_env("RECONCILE_BUCKET", "10000")),
        )

    @classmethod
    def from_connections(cls, mariadb, ch_admin, ch_loader, ch_analyst) -> "Settings":
        """Под Airflow: адреса и пароли из Connections (на стенде их задаёт docker-compose
        из .env, в проде secrets backend, например Vault). Параметры загрузки из окружения."""
        return cls(
            mariadb_host=mariadb.host,
            mariadb_port=int(mariadb.port or 3306),
            mariadb_db=mariadb.schema or "app",
            mariadb_user=mariadb.login,
            mariadb_password=mariadb.password,
            ch_host=ch_loader.host,
            ch_port=int(ch_loader.port or 8123),
            ch_admin_user=ch_admin.login,
            ch_admin_password=ch_admin.password,
            ch_loader_user=ch_loader.login,
            ch_loader_password=ch_loader.password,
            ch_analyst_user=ch_analyst.login,
            ch_analyst_password=ch_analyst.password,
            results_dir=_env("RESULTS_DIR", "/results"),
            load_chunk_rows=int(_env("LOAD_CHUNK_ROWS", "50000")),
            load_overlap_ids=int(_env("LOAD_OVERLAP_IDS", "1000")),
            reconcile_hash_days=int(_env("RECONCILE_HASH_DAYS", "30")),
            reconcile_bucket=int(_env("RECONCILE_BUCKET", "10000")),
        )

    def ddl_vars(self) -> dict[str, str]:
        """Плейсхолдеры {{VAR}} для DDL-файлов (пользователи и пароли)."""
        return {
            "CLICKHOUSE_LOADER_USER": self.ch_loader_user,
            "CLICKHOUSE_LOADER_PASSWORD": self.ch_loader_password,
            "CLICKHOUSE_ANALYST_USER": self.ch_analyst_user,
            "CLICKHOUSE_ANALYST_PASSWORD": self.ch_analyst_password,
        }
