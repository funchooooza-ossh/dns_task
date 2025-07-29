import logging
from typing import Any

import structlog
from structlog.processors import (
	EventRenamer,
	JSONRenderer,
	TimeStamper,
)
from structlog.stdlib import PositionalArgumentsFormatter, add_log_level


def add_app_name(
	logger,  # noqa: ANN001
	method_name: str,
	event_dict: dict[str, Any],
) -> dict[str, Any]:
	event_dict["app_name"] = "dns_task"
	return event_dict


def setup_logger() -> None:
	logging.root.handlers.clear()
	processors = [
		add_app_name,
		TimeStamper(fmt="iso"),
		add_log_level,
		PositionalArgumentsFormatter(),
		EventRenamer("msg"),
		JSONRenderer(),
	]

	structlog.configure(
		processors=processors,
		context_class=dict,
		logger_factory=structlog.PrintLoggerFactory(),
		wrapper_class=structlog.BoundLogger,
		cache_logger_on_first_use=True,
	)
	logger = structlog.get_logger()
	logger.debug("loggin configured", stage="startup")
