"""Подключения к ClickHouse (HTTP) и MariaDB."""
import clickhouse_connect
import pymysql

from pipeline.config import Settings


def ch_admin(s: Settings):
    """Администратор: только для DDL и пользователей."""
    return clickhouse_connect.get_client(
        host=s.ch_host, port=s.ch_port, username=s.ch_admin_user, password=s.ch_admin_password
    )


def ch_loader(s: Settings):
    """Пользователь загрузки: всё, что делает пайплайн после DDL."""
    return clickhouse_connect.get_client(
        host=s.ch_host, port=s.ch_port, username=s.ch_loader_user, password=s.ch_loader_password
    )


def mariadb(s: Settings):
    """Источник. Только чтение; autocommit, чтобы не держать транзакции на реплике."""
    return pymysql.connect(
        host=s.mariadb_host,
        port=s.mariadb_port,
        user=s.mariadb_user,
        password=s.mariadb_password,
        database=s.mariadb_db,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.Cursor,
    )
