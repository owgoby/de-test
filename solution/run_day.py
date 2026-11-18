#!/usr/bin/env python3
"""Сутки на стенде: прогон на исходном состоянии, затем шесть пакетов `make next`,
после каждого снова прогон. Выгрузки после 1-го, 4-го и 7-го прогонов.

Прогон это DAG `revenue` в Airflow, тот же, что идёт по расписанию, запущенный синхронно
(`airflow dags test`). Скрипт только имитирует время и делает выгрузки, логики в нём нет.

Запуск из корня репозитория, нужен только Python 3 без зависимостей:

    make reset && python3 solution/run_day.py            # сутки и три выгрузки
    make reset && python3 solution/run_day.py --verify   # плюс сверка с эталоном после каждого прогона

`--verify` это тестовый режим стенда: он читает MariaDB целиком, в проде его нет.
"""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BATCHES = 6
EXPORT_AFTER_BATCH = {0: "state0", 3: "state3", 6: "final"}  # пакетов применено -> имя выгрузки


def sh(*cmd: str, quiet: bool = False) -> None:
    """Команда в корне репозитория; при ошибке скрипт останавливается с её кодом."""
    result = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.DEVNULL if quiet else None)
    if result.returncode != 0:
        sys.exit(f"!!! команда завершилась с кодом {result.returncode}: {' '.join(cmd)}")


def run_pipeline() -> None:
    sh("docker", "compose", "exec", "-T", "airflow", "airflow", "dags", "test", "revenue", quiet=True)


def pipeline_cli(*args: str) -> None:
    sh("docker", "compose", "run", "--rm", "pipeline", *args)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--verify", action="store_true", help="после каждого прогона сверять витрину с эталоном")
    args = parser.parse_args()

    for batch in range(BATCHES + 1):
        if batch > 0:
            sh("make", "next")
        print(f"=== прогон {batch + 1}: пакетов применено {batch}", flush=True)
        run_pipeline()
        if args.verify:
            pipeline_cli("verify")
        if batch in EXPORT_AFTER_BATCH:
            pipeline_cli("export", EXPORT_AFTER_BATCH[batch])

    print("=== готово:", ", ".join(f"results/revenue_{name}.csv" for name in EXPORT_AFTER_BATCH.values()))


if __name__ == "__main__":
    main()
