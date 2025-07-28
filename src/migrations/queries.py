from collections import OrderedDict

DEFAULT_SCHEMA = """
				CREATE SCHEMA IF NOT EXISTS logistics;
				"""

DEFAULT_HISTORY = """
					CREATE TABLE IF NOT EXISTS logistics.branch_product_history (
						id SERIAL PRIMARY KEY,
						date DATE NOT NULL,
						branch_id UUID NOT NULL,
						product_id UUID NOT NULL,
						stock NUMERIC NOT NULL,
						reserved NUMERIC NOT NULL,
						in_transit NUMERIC NOT NULL
					);

					"""

DEFAULT_RC_HISTORY = """
					CREATE TABLE IF NOT EXISTS logistics.rc_product_history (
						id SERIAL PRIMARY KEY,
						date DATE NOT NULL,
						product_id UUID NOT NULL,
						stock NUMERIC NOT NULL,
						reserved NUMERIC NOT NULL,
						in_transit NUMERIC NOT NULL
					);
					"""

DEFAULT_NEEDS = """
				CREATE TABLE IF NOT EXISTS logistics.needs (
					id SERIAL PRIMARY KEY,
					branch_id UUID NOT NULL,
					product_id UUID NOT NULL,
					needs NUMERIC NOT NULL
				);
				"""


DEFAULT_LOGDAYS = """
					CREATE TABLE IF NOT EXISTS logistics.logdays (
						id SERIAL PRIMARY KEY,
						branch_id UUID NOT NULL,
						category_id UUID NOT NULL,
						logdays INT NOT NULL CHECK (logdays IN (7, 14, 21))
					);
					"""


DEFAULT_SHIPMENT = """
					CREATE TABLE IF NOT EXISTS logistics.min_shipment (
						id SERIAL PRIMARY KEY,
						branch_id UUID NOT NULL,
						product_id UUID NOT NULL,
						min_qty NUMERIC NOT NULL
					);
					"""

DEFAULT_LIMITS = """
				CREATE TABLE IF NOT EXISTS logistics.storage_limits (
					id SERIAL PRIMARY KEY,
					branch_id UUID NOT NULL,
					max_volume NUMERIC NOT NULL
				);
				"""


MIGRATIONS = OrderedDict(
	{
		"001_create_schema": DEFAULT_SCHEMA,
		"002_create_branch_history": DEFAULT_HISTORY,
		"003_create_rc_history": DEFAULT_RC_HISTORY,
		"004_create_needs": DEFAULT_NEEDS,
		"005_create_logdays": DEFAULT_LOGDAYS,
		"006_create_min_shipment": DEFAULT_SHIPMENT,
		"007_create_storage_limits": DEFAULT_LIMITS,
	}
)
