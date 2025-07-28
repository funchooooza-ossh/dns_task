from pydantic import BaseModel


class Schemas(BaseModel):
	schemas: list[str]
