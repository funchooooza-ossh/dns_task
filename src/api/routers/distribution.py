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
	respect_volume: bool = Query(default=False, description="Учет объема"),
	schema: str = Query(default="logistics", description="Схема в БД"),
	rc_table: str = Query(default="rc_product_history"),
	branch_table: str = Query(default="branch_product_history"),
	needs_table: str = Query(default="needs"),
	min_table: str = Query(default="min_shipment"),
	volume_table: str = Query(default="products_vol"),
	limit_table: str = Query(default="storage_limits"),
	product_table: str = Query(default="products"),
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
			respect_volume=respect_volume,
			schema=schema,
			rc_table=rc_table,
			branch_table=branch_table,
			needs_table=needs_table,
			min_table=min_table,
			volume_table=volume_table,
			limit_table=limit_table,
			product_table=product_table,
		)
	return [DistributionRow(**row) for row in rows]
