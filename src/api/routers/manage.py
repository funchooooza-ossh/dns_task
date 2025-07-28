from fastapi import APIRouter

from api.deps.db import Pool
from api.dto.manage import Schemas
from core.factories.db import acquire_connection
from core.workflow.datasources import get_schemas

router = APIRouter(tags=["manage"])


@router.get("/manage/schemas", response_model=Schemas)
async def schemas(pool: Pool) -> Schemas:
	async with acquire_connection(pool) as conn:
		schemas = await get_schemas(conn)

	return Schemas(schemas=schemas)
