from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel


class DistributionRow(BaseModel):
	branch_id: UUID
	product_id: UUID
	demand: Decimal
	available: Decimal
	qty: Decimal
