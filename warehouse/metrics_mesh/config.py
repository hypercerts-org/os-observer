import os

import dotenv
from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.config.connection import (
    GCPPostgresConnectionConfig,
    TrinoConnectionConfig,
)

dotenv.load_dotenv()


config = Config(
    model_defaults=ModelDefaultsConfig(dialect="clickhouse", start="2024-08-01"),
    gateways={
        "local": GatewayConfig(
            connection=DuckDBConnectionConfig(
                database=os.environ.get("SQLMESH_DUCKDB_LOCAL_PATH")
            ),
            variables={
                "oso_source": "sources",
            },
        ),
        "trino": GatewayConfig(
            connection=TrinoConnectionConfig(
                host=os.environ.get("SQLMESH_TRINO_HOST", "localhost"),
                port=int(os.environ.get("SQLMESH_TRINO_PORT", "8080")),
                http_scheme="http",
                user=os.environ.get("SQLMESH_TRINO_USER", "sqlmesh"),
                catalog="metrics",
                concurrent_tasks=int(
                    os.environ.get("SQLMESH_TRINO_CONCURRENT_TASKS", "64")
                ),
            ),
            state_connection=GCPPostgresConnectionConfig(
                instance_connection_string=os.environ.get(
                    "SQLMESH_POSTGRES_INSTANCE_CONNECTION_STRING", ""
                ),
                user=os.environ.get("SQLMESH_POSTGRES_USER", ""),
                password=os.environ.get("SQLMESH_POSTGRES_PASSWORD", "placeholder"),
                db=os.environ.get("SQLMESH_POSTGRES_DB", ""),
            ),
            variables={
                "oso_source": "default",
            },
        ),
    },
    default_gateway="local",
    variables={"fulltime_dev_days": 10, "activity_window": 30},
)
