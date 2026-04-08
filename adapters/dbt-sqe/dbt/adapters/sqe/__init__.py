from dbt.adapters.sqe.connections import SQECredentials  # noqa: F401
from dbt.adapters.sqe.connections import SQEConnectionManager  # noqa: F401
from dbt.adapters.sqe.impl import SQEAdapter
from dbt.adapters.base import AdapterPlugin
from dbt.include.sqe import PACKAGE_PATH

Plugin = AdapterPlugin(
    adapter=SQEAdapter,
    credentials=SQECredentials,
    include_path=PACKAGE_PATH,
)
