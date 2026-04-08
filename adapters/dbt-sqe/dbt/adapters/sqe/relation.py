"""SQE relation (table/view reference)."""

from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType


@dataclass(frozen=True, eq=False, repr=False)
class SQERelation(BaseRelation):
    quote_character: str = '"'

    def render(self) -> str:
        parts = []
        if self.database:
            parts.append(self.quoted(self.database))
        if self.schema:
            parts.append(self.quoted(self.schema))
        if self.identifier:
            parts.append(self.quoted(self.identifier))
        return ".".join(parts)

    def quoted(self, identifier: str) -> str:
        return f'{self.quote_character}{identifier}{self.quote_character}'
