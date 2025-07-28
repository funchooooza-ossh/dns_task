from pydantic import BaseModel


class Schemas(BaseModel):
	schemas: list[str]


class Field(BaseModel):
	name: str
	type: str


class Table(BaseModel):
	name: str
	fields: list[Field]


class Tables(BaseModel):
	tables: list[Table]
