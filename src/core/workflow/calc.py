from datetime import date as Date
from typing import Any
from uuid import UUID

import asyncpg


async def calculate_distribution(
	conn: asyncpg.Connection,
	date: Date | None = None,
	branch_id: UUID | None = None,
	product_id: UUID | None = None,
	category_id: UUID | None = None,
	min_demand: float | None = None,
	respect_volume: bool = False,
	limit: int | None = None,
) -> list[asyncpg.Record]:
	date = date or Date.today()
	args: list[Any] = [date]
	conditions = []
	joins = []

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
		joins.append("JOIN logistics.products p ON j.product_id = p.product_id")
		conditions.append(f"p.category_id = ${param_index}")
		args.append(category_id)
		param_index += 1
	if min_demand is not None:
		conditions.append(f"j.demand >= ${param_index}")
		args.append(min_demand)
		param_index += 1

	where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
	join_clause = "\n".join(joins)
	limit_clause = f"LIMIT {limit}" if limit else ""

	query = f"""
        WITH rc_available AS (
            SELECT
                product_id,
                SUM(stock - reserved - in_transit) AS available
            FROM logistics.rc_product_history
            WHERE date = $1
            GROUP BY product_id
        ),

        branch_demand AS (
            SELECT
                n.branch_id,
                n.product_id,
                n.needs - COALESCE(h.stock, 0) - COALESCE(h.in_transit, 0) AS demand
            FROM logistics.needs n
            LEFT JOIN logistics.branch_product_history h
                ON h.branch_id = n.branch_id AND h.product_id = n.product_id AND h.date = $1
            WHERE n.needs - COALESCE(h.stock, 0) - COALESCE(h.in_transit, 0) > 0
        ),

        min_filtered AS (
            SELECT
                bd.*,
                m.min_qty
            FROM branch_demand bd
            JOIN logistics.min_shipment m
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

        SELECT j.*
        FROM joined j
        {join_clause}
        {where_clause}
        ORDER BY product_id, branch_id
        {limit_clause}
    """  # noqa: S608

	return await conn.fetch(query, *args)
