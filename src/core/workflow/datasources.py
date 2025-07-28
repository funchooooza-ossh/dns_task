from collections import defaultdict

import asyncpg

from core.workflow.dataclasses import FieldMeta, SchemaMeta, TableMeta


async def get_schemas(conn: asyncpg.Connection) -> list[str]:
	QUERY = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name IN ('logistics')
        ORDER BY schema_name
    """
	rows = await conn.fetch(QUERY)
	return [row["schema_name"] for row in rows]


async def get_schema_structure(conn: asyncpg.Connection, schema: str) -> SchemaMeta:
	QUERY = """
    SELECT
        table_name,
        column_name,
        data_type
    FROM information_schema.columns
    WHERE table_schema = $1
    ORDER BY table_name, ordinal_position
    """
	rows = await conn.fetch(QUERY, schema)

	grouped: dict[str, list[FieldMeta]] = defaultdict(list)
	for row in rows:
		grouped[row["table_name"]].append(
			FieldMeta(name=row["column_name"], type=row["data_type"])
		)

	tables = [TableMeta(name=table, fields=fields) for table, fields in grouped.items()]
	return SchemaMeta(tables=tables)
