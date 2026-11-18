"""Применение DDL: файлы solution/ddl/*.sql по порядку имён, каждый запрос отдельно.
Все запросы идемпотентны (IF NOT EXISTS / OR REPLACE / ALTER), поэтому шаг запускается
при каждом прогоне и на чистом сервере создаёт всё с нуля."""
import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)
DDL_DIR = Path(__file__).resolve().parent.parent / "ddl"


FORBIDDEN = (";", "--", "'", "\\", "\n")


def _render(sql: str, variables: dict[str, str]) -> str:
    """Значения попадают в SQL как есть, поэтому символы, способные разорвать запрос
    или изменить его смысл, запрещены заранее и с понятной ошибкой."""
    def repl(m: re.Match) -> str:
        key = m.group(1)
        if key not in variables:
            raise RuntimeError(f"в DDL нет значения для плейсхолдера {key}")
        value = variables[key]
        if any(ch in value for ch in FORBIDDEN):
            raise RuntimeError(f"значение {key} не должно содержать ; -- ' \\ и перевод строки")
        return value

    return re.sub(r"\{\{(\w+)\}\}", repl, sql)


def statements(sql: str) -> list[str]:
    """Файл режется на запросы по ';' в конце строки, комментарии удаляются."""
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    body = "\n".join(line.split(" -- ")[0] for line in lines)
    return [st.strip() for st in body.split(";") if st.strip()]


def apply_ddl(client, variables: dict[str, str]) -> int:
    count = 0
    for path in sorted(DDL_DIR.glob("*.sql")):
        for statement in statements(_render(path.read_text(encoding="utf-8"), variables)):
            client.command(statement)
            count += 1
        log.info("DDL применён: %s", path.name)
    return count
