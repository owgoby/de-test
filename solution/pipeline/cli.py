"""Точка входа: python -m pipeline <команда>.

  run            штатный прогон: DDL -> загрузка -> витрина (с проверками до публикации) -> инварианты
  ddl            только DDL и пользователи
  load           только загрузка источника в raw
  mart           только пересборка витрины
  check          только инварианты витрины внутри ClickHouse
  audit          полная сверка с хешем по всей таблице (в проде раз в сутки)
  verify         стенд: сверка stg с источником и эталон на pandas; читает MariaDB целиком
  export <name>  выгрузить mart.revenue в /results/revenue_<name>.csv
"""
import logging
import sys
import time
import uuid

from pipeline import ch
from pipeline.config import Settings
from pipeline.ddl import apply_ddl
from pipeline.load import audit_all, load_all
from pipeline.mart import build_revenue, export_revenue
from pipeline.checks import run_checks, verify_against_source

log = logging.getLogger("pipeline")


def step_ddl(s: Settings) -> None:
    n = apply_ddl(ch.ch_admin(s), s.ddl_vars())
    log.info("DDL: %d запросов применено", n)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout
    )
    command = argv[0] if argv else "run"
    s = Settings.from_env()
    t0 = time.monotonic()
    ok = True
    if command == "run":
        run_id = uuid.uuid4()  # один на прогон: загрузка и проверки в meta связаны, как под Airflow
        step_ddl(s)
        load_all(s, run_id)
        build_revenue(s, run_id)
        ok = run_checks(s, run_id)
    elif command == "ddl":
        step_ddl(s)
    elif command == "load":
        load_all(s)
    elif command == "mart":
        build_revenue(s)
    elif command == "check":
        ok = run_checks(s)
    elif command == "audit":
        audit_all(s)
    elif command == "verify":
        ok = verify_against_source(s)
    elif command == "export":
        if len(argv) < 2:
            print(__doc__)
            return 2
        export_revenue(s, argv[1])
    else:
        print(__doc__)
        return 2
    log.info("готово за %.1f с", time.monotonic() - t0)
    return 0 if ok else 1
