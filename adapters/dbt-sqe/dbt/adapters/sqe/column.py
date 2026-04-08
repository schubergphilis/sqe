"""SQE column type mapping."""

from dbt.adapters.base.column import Column


class SQEColumn(Column):
    """Maps Iceberg/Arrow type names to SQL standard types for dbt."""

    TYPE_LABELS = {
        "STRING": "VARCHAR",
        "LONG": "BIGINT",
        "SHORT": "SMALLINT",
        "BYTE": "TINYINT",
        "FLOAT": "REAL",
    }

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return cls.TYPE_LABELS.get(dtype.upper(), dtype.upper())

    def is_string(self) -> bool:
        return self.dtype.upper() in ("VARCHAR", "TEXT", "STRING", "CHAR", "UTF8")

    def is_integer(self) -> bool:
        return self.dtype.upper() in (
            "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
            "INT32", "INT64", "INT16", "INT8", "LONG",
        )

    def is_float(self) -> bool:
        return self.dtype.upper() in ("FLOAT", "DOUBLE", "REAL", "FLOAT32", "FLOAT64")

    def is_numeric(self) -> bool:
        return self.is_integer() or self.is_float() or self.dtype.upper().startswith("DECIMAL")
