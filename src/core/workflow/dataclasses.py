from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FieldMeta:
	name: str
	type: str


@dataclass(frozen=True, slots=True)
class TableMeta:
	name: str
	fields: list[FieldMeta]


@dataclass(frozen=True, slots=True)
class SchemaMeta:
	tables: list[TableMeta]
