"""SQE adapter implementation."""

import agate
from dbt.adapters.base.meta import available
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sqe.connections import SQEConnectionManager
from dbt.adapters.sqe.column import SQEColumn
from dbt.adapters.sqe.relation import SQERelation


class SQEAdapter(SQLAdapter):
    """dbt adapter for SQE (Sovereign Query Engine)."""

    ConnectionManager = SQEConnectionManager
    Column = SQEColumn
    Relation = SQERelation

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    def valid_incremental_strategies(self):
        return ["append", "delete+insert", "merge"]

    # A materialization that writes the target in several statements wraps
    # them in one SQE transaction, so readers never see a half-applied model.
    # DDL and CTAS stay outside it (they autocommit). See _Handle for how a
    # failure rolls it back.
    @available
    def begin_transaction(self) -> str:
        self.connections.get_thread_connection().handle.begin_transaction()
        return ""

    @available
    def commit_transaction(self) -> str:
        self.connections.get_thread_connection().handle.commit_transaction()
        return ""

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "VARCHAR"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        if decimals and decimals > 0:
            return "DOUBLE"
        else:
            return "BIGINT"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "BOOLEAN"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "TIMESTAMP"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "DATE"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "VARCHAR"
