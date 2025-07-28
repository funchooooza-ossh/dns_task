import asyncpg


async def get_schemas(conn: asyncpg.Connection) -> list[str]:
	QUERY = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public', 'pg_toast')
        ORDER BY schema_name
    """
	rows = await conn.fetch(QUERY)
	return [row["schema_name"] for row in rows]
