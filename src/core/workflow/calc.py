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
	logdays_table: str = "logdays",
) -> list[asyncpg.Record]:
	date = date or Date.today()
	args: list[Any] = [date]
	conditions = []
	joins = []
	cte_volume = ""
	param_index = 2

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
                GREATEST(n.needs - COALESCE(h.stock, 0) - COALESCE(h.in_transit, 0), 0) AS base_demand
            FROM {schema}.{needs_table} n
            LEFT JOIN {schema}.{branch_table} h
                ON h.branch_id = n.branch_id AND h.product_id = n.product_id AND h.date = $1
        ),

        demand_with_min AS (
            SELECT
                bd.branch_id,
                bd.product_id,
                GREATEST(bd.base_demand, m.min_qty) AS demand,
                m.min_qty
            FROM branch_demand bd
            JOIN {schema}.{min_table} m
                ON m.branch_id = bd.branch_id AND m.product_id = bd.product_id
        ),

        logdays_adjusted AS (
            SELECT
                d.branch_id,
                d.product_id,
                d.demand * (1 + COALESCE(ld.logdays, 7)::float / 30.0) AS adjusted_demand,
                d.min_qty,
				ld.logdays AS logdays
            FROM demand_with_min d
			LEFT JOIN {schema}.{product_table} p ON d.product_id = p.product_id
            LEFT JOIN {schema}.{logdays_table} ld
				ON d.branch_id = ld.branch_id AND p.category_id = ld.category_id
        ),

        joined AS (
            SELECT
                la.branch_id,
                la.product_id,
                la.adjusted_demand as demand,
                la.min_qty,
                rc.available,
                LEAST(la.adjusted_demand, rc.available) AS qty,
				la.logdays as logdays
            FROM logdays_adjusted la
            JOIN rc_available rc USING (product_id)
            WHERE rc.available > 0
        )
        {cte_volume}

        SELECT j.*
        FROM joined j
        {"\n".join(joins)}
        {"WHERE " + " AND ".join(conditions) if conditions else ""}
        ORDER BY product_id, branch_id
        {f"LIMIT {limit}" if limit else ""}
    """  # noqa: S608

	return await conn.fetch(query, *args)
