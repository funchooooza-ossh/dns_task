import os
from dataclasses import dataclass, field
from urllib.parse import urlparse


def str2bool(value: str) -> bool:
	return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class AppCfg:
	pg_url: str = field(default_factory=lambda: os.getenv("POSTGRES_URL", ""))
	production: bool = field(
		default_factory=lambda: str2bool(os.getenv("PRODUCTION", "false"))
	)

	def __post_init__(self) -> None:
		parsed = urlparse(self.pg_url)

		if not self.pg_url:
			raise ValueError("POSTGRES_URL is empty. Set it via .env or env variable.")

		if parsed.scheme not in ("postgres", "postgresql"):
			raise ValueError(
				f"Invalid scheme in POSTGRES_URL: '{parsed.scheme}'. Must be 'postgresql://'"
			)

		if not parsed.hostname or not parsed.path:
			raise ValueError("Incomplete POSTGRES_URL: missing host or database name.")


cfg = AppCfg()
