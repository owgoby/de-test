"""Общее для тестов на черновом стенде.

Тесты меняют MariaDB своими SQL-файлами (sql/), чтобы проверить свойства источника, которых нет
в шести пакетах стенда. Правило «MariaDB только читайте» относится к пайплайну, а не к тестам стенда.
Запускаются на свежем стенде, после них стенд возвращают командой `make reset`.
"""
from pathlib import Path

import pymysql
import pytest
from pymysql.constants import CLIENT

from pipeline import ch, cli
from pipeline.checks import Checker
from pipeline.config import Settings

SQL_DIR = Path(__file__).parent / "sql"


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.from_env()


@pytest.fixture(scope="session")
def source(settings):
    """Отдельное подключение к MariaDB, которому разрешены SQL-файлы из нескольких запросов."""
    conn = pymysql.connect(
        host=settings.mariadb_host, port=settings.mariadb_port, user=settings.mariadb_user,
        password=settings.mariadb_password, database=settings.mariadb_db,
        autocommit=True, client_flag=CLIENT.MULTI_STATEMENTS,
    )
    yield conn
    conn.close()


class Stand:
    """Действия над стендом, из которых складываются сценарии."""

    def __init__(self, settings, source):
        self.s, self.src = settings, source
        # На свежем стенде пользователя loader ещё нет: его создаёт DDL (идемпотентно).
        self.pipeline("ddl")
        self.ch = ch.ch_loader(settings)

    def pipeline(self, command: str = "run") -> None:
        """Тот же код, что вызывают задачи DAG, без Airflow: тесты проверяют логику, а не расписание."""
        assert cli.main([command]) == 0, f"pipeline {command} завершился с ошибкой"

    def verify(self) -> None:
        """Сверка stg с источником и витрины с эталоном на pandas."""
        checker = Checker(self.s)
        assert checker.verify(), f"расхождения с эталоном: {checker.failed}"

    def apply(self, name: str) -> None:
        with self.src.cursor() as cur:
            cur.execute((SQL_DIR / name).read_text(encoding="utf-8"))
            while cur.nextset():
                pass

    def src_value(self, sql: str):
        with self.src.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else None

    def ch_value(self, sql: str, **params):
        return self.ch.query(sql, parameters=params).result_rows[0][0]

    def mart_rows_with_payment(self, payment_uid: str) -> int:
        return self.ch_value("SELECT count() FROM mart.revenue WHERE payment_uid = {uid:String}", uid=payment_uid)


@pytest.fixture(scope="session")
def stand(settings, source) -> Stand:
    st = Stand(settings, source)
    if st.src_value("SELECT COUNT(*) FROM payments WHERE payment_uid LIKE 'eeeeeeee-%'"):
        pytest.exit("источник уже изменён прошлым запуском тестов: сначала make reset", returncode=2)
    return st
