#!/usr/bin/env python3
"""Создаёт .env в корне репозитория из .env.example: каждое <generate> заменяется
случайным паролем. Если .env уже есть, ничего не делает (пароли не меняются под живым стендом).

    python3 solution/init_env.py

Пароли из hex-символов: в них нет символов, которые запрещает подстановка в DDL."""
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLACEHOLDER = "<generate>"


def main() -> None:
    env, example = ROOT / ".env", ROOT / ".env.example"
    if env.exists():
        print(f"{env} уже есть, оставляю как есть")
        return
    if not example.exists():
        sys.exit(f"нет шаблона {example}")
    lines = [line.replace(PLACEHOLDER, secrets.token_hex(16)) if PLACEHOLDER in line else line
             for line in example.read_text(encoding="utf-8").splitlines(keepends=True)]
    env.write_text("".join(lines), encoding="utf-8")
    env.chmod(0o600)
    print(f"создан {env}")


if __name__ == "__main__":
    main()
