from fastapi import APIRouter, Query

from api.deps.db import Pool
from api.dto.manage import Field, Schemas, Table, Tables
from core.factories.db import acquire_connection
from core.workflow.datasources import get_schema_structure, get_schemas

router = APIRouter(tags=["manage"])


@router.get("/manage/schemas", response_model=Schemas)
async def schemas(pool: Pool) -> Schemas:
	async with acquire_connection(pool) as conn:
		schemas = await get_schemas(conn)

	return Schemas(schemas=schemas)


@router.get("/manage/tables", response_model=Tables)
async def tables(
	pool: Pool,
	schema: str = Query(
		default="logistics",
		alias="schema",
		title="Имя схемы",
		description="Имя схемы для получения структур столов",
	),
) -> Tables:
	async with acquire_connection(pool) as conn:
		meta = await get_schema_structure(conn, schema=schema)

	return Tables(
		tables=[
			Table(
				name=t.name,
				fields=[Field(name=f.name, type=f.type) for f in t.fields],
			)
			for t in meta.tables
		]
	)
