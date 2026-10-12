"""SQE relation (table/view reference)."""

from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation


# BaseRelation.render quotes every part (the default quote policy) and honours
# include(): dbt-core lists schemas with str(relation.include(schema=False)),
# which must render the catalog alone.
@dataclass(frozen=True, eq=False, repr=False)
class SQERelation(BaseRelation):
    quote_character: str = '"'
