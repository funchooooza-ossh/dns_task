from datetime import date as Date
from typing import Any
from uuid import UUID

import asyncpg
import structlog

logger = structlog.get_logger()


async def calculate_distribution(
	conn: asyncpg.Connection,
	date: Date | None = None,
	branch_id: UUID | None = None,
	product_id: UUID | None = None,
	category_id: UUID | None = None,
	min_demand: float | None = None,
	respect_volume: bool = False,
	limit: int | None = None,
	schema: str = "logistics",
	rc_table: str = "rc_product_history",
	branch_table: str = "branch_product_history",
	needs_table: str = "needs",
	min_table: str = "min_shipment",
	volume_table: str = "products_vol",
	limit_table: str = "storage_limits",
	product_table: str = "products",
) -> list[asyncpg.Record]:
	"""
	Расчет оптимального распределения товаров из распределительного центра (РЦ) в магазины сети.

	Алгоритм:
	1. Считает доступные остатки на РЦ (с учетом резерва и транзита) на указанную дату.
	2. Вычисляет спрос в магазинах: needs - текущие остатки - в пути.
	3. Отбрасывает магазины, где спрос ниже минимального объема отгрузки (`min_shipment`).
	4. Сопоставляет доступный объем на РЦ и потребность магазинов, определяет qty = min(demand, available).
	5. Применяет опциональные фильтры:
		- По филиалу (`branch_id`)
		- По товару (`product_id`)
		- По категории (`category_id`)
		- По минимальному спросу (`min_demand`)
	6. Возвращает список записей: куда, что и в каком объеме отгружать.

	Параметры:
		conn (asyncpg.Connection): подключение к базе данных.
		date (Date | None): дата расчета, по умолчанию — сегодня.
		branch_id (UUID | None): фильтр по филиалу.
		product_id (UUID | None): фильтр по товару.
		category_id (UUID | None): фильтр по категории.
		min_demand (float | None): отфильтровать строки с низким спросом.
		respect_volume (bool): учитывать ограничения по объему (не реализовано).
		limit (int | None): ограничить количество строк в выдаче.

	Возвращает:
		list[asyncpg.Record]: записи распределения с полями:
			- branch_id
			- product_id
			- demand (спрос)
			- available (доступно на РЦ)
			- qty (сколько реально отгрузить)
	"""
	date = date or Date.today()
	args: list[Any] = [date]
	conditions = []
	joins = []
	cte_volume = ""

	param_index = 2  # $1 занят под date

	if branch_id:
		conditions.append(f"j.branch_id = ${param_index}")
		args.append(branch_id)
		param_index += 1
	if product_id:
		conditions.append(f"j.product_id = ${param_index}")
		args.append(product_id)
		param_index += 1
	if category_id:
		joins.append(f"JOIN {schema}.{product_table} p ON j.product_id = p.product_id")
		conditions.append(f"p.category_id = ${param_index}")
		args.append(category_id)
		param_index += 1
	if min_demand is not None:
		conditions.append(f"j.demand >= ${param_index}")
		args.append(min_demand)
		param_index += 1

	if respect_volume:
		cte_volume = f"""
			, free_volume AS (
				SELECT
					sl.branch_id,
					sl.max_volume - COALESCE(SUM(h.stock * pv.volume_per_unit), 0) AS available_volume
				FROM {schema}.{limit_table} sl
				LEFT JOIN {schema}.{branch_table} h
					ON h.branch_id = sl.branch_id AND h.date = $1
				LEFT JOIN {schema}.{volume_table} pv
					ON h.product_id = pv.product_id
				GROUP BY sl.branch_id, sl.max_volume
			)
			"""  # noqa: S608
		joins.append(f"JOIN {schema}.{volume_table} v ON j.product_id = v.product_id")
		joins.append("JOIN free_volume fv ON j.branch_id = fv.branch_id")
		conditions.append("j.qty * v.volume_per_unit <= fv.available_volume")

	where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
	join_clause = "\n".join(joins)
	limit_clause = f"LIMIT {limit}" if limit else ""

	query = f"""
			WITH rc_available AS (
				SELECT
					product_id,
					SUM(stock - reserved - in_transit) AS available
				FROM {schema}.{rc_table}
				WHERE date = $1
				GROUP BY product_id
			),

			branch_demand AS (
				SELECT
					n.branch_id,
					n.product_id,
					n.needs - COALESCE(h.stock, 0) - COALESCE(h.in_transit, 0) AS demand
				FROM {schema}.{needs_table} n
				LEFT JOIN {schema}.{branch_table} h
					ON h.branch_id = n.branch_id AND h.product_id = n.product_id AND h.date = $1
				WHERE n.needs - COALESCE(h.stock, 0) - COALESCE(h.in_transit, 0) > 0
			),

			min_filtered AS (
				SELECT
					bd.*,
					m.min_qty
				FROM branch_demand bd
				JOIN {schema}.{min_table} m
					ON m.branch_id = bd.branch_id AND m.product_id = bd.product_id
				WHERE bd.demand >= m.min_qty
			),

			joined AS (
				SELECT
					mf.branch_id,
					mf.product_id,
					mf.demand,
					rc.available,
					LEAST(mf.demand, rc.available) AS qty
				FROM min_filtered mf
				JOIN rc_available rc USING (product_id)
				WHERE rc.available > 0
			)
			{cte_volume}

			SELECT j.*
			FROM joined j
			{join_clause}
			{where_clause}
			ORDER BY product_id, branch_id
			{limit_clause}
		"""  # noqa: S608

	return await conn.fetch(query, *args)
