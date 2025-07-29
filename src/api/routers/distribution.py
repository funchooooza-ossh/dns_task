from datetime import date
from uuid import UUID

from fastapi import APIRouter, Query

from api.deps.db import Pool
from api.dto.distribution import DistributionRow
from core.factories.db import acquire_connection
from core.workflow.calc import calculate_distribution

router = APIRouter(tags=["distribution"])


@router.get("/distribution", response_model=list[DistributionRow])
async def get_distribution(
	pool: Pool,
	run_date: date | None = Query(
		default=None, description="Дата расчета (по умолчанию — сегодня)"
	),
	branch_id: UUID | None = Query(default=None, description="Фильтр по филиалу"),
	product_id: UUID | None = Query(default=None, description="Фильтр по товару"),
	category_id: UUID | None = Query(default=None, description="Фильтр по категории"),
	min_demand: float | None = Query(default=None, description="Минимальный спрос"),
	limit: int | None = Query(default=None, description="Лимит записей"),
) -> list[DistributionRow]:
	async with acquire_connection(pool) as conn:
		rows = await calculate_distribution(
			conn=conn,
			date=run_date,
			branch_id=branch_id,
			product_id=product_id,
			category_id=category_id,
			min_demand=min_demand,
			limit=limit,
		)

	return [DistributionRow(**row) for row in rows]
