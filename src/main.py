import asyncio

import structlog
import typer

from config import AppCfg
from etl.migrate import migrate
from logger import setup_logger

setup_logger()

app = typer.Typer()
logger = structlog.get_logger()


@app.command("migrate")
def run_migrations() -> None:
	"""Запуск миграций вручную через CLI"""
	logger.info("Starting migrations...", stage="startup")
	asyncio.run(migrate())
	logger.info("Migrations complete.", stage="startup")


@app.command("ping-db")
def ping_db() -> None:
	"""Проверка, что конфиг валиден"""
	cfg = AppCfg()
	logger.info("PostgreSQL URL", url=cfg.pg_url)


if __name__ == "__main__":
	app()
