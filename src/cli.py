import asyncio
import subprocess
from pathlib import Path

import structlog
import typer
import uvicorn

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


@app.command("serve")
def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = True) -> None:  # noqa: S104
	"""Запуск FastAPI-приложения"""
	logger.info("Starting web server...", host=host, port=port)
	uvicorn.run("main:app", host=host, port=port, reload=reload)


@app.command("visualize")
def run_streamlit() -> None:
	"""Запуск визуализации через Streamlit в фоне"""
	import sys

	logger.info("Запуск Streamlit интерфейса...")

	streamlit_app = Path(__file__).parent / "frontend" / "app.py"

	if not streamlit_app.exists():
		logger.error("Streamlit UI файл не найден", path=str(streamlit_app))
		raise FileNotFoundError(f"{streamlit_app} не найден")

	try:
		subprocess.Popen(  # noqa: S603
			[sys.executable, "-m", "streamlit", "run", str(streamlit_app)],
			start_new_session=True,
			stdout=subprocess.DEVNULL,
			stderr=subprocess.DEVNULL,
		)
		logger.info("Streamlit запущен в фоне по адресу http://localhost:8501")
	except FileNotFoundError:
		logger.error("Streamlit не найден. Установи через pip install streamlit")
		raise


if __name__ == "__main__":
	app()
