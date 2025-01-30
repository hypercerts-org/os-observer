import logging
import os
import typing as t
from contextlib import contextmanager
from datetime import datetime

import duckdb
import psycopg2
from google.cloud import bigquery
from pydantic import BaseModel, Field
from sqlglot import exp
from sqlmesh.core.dialect import parse_one

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID", "opensource-observer")

class RowRestriction(BaseModel):
    time_column: str = ""
    # Other expressions that are used in the row restriction. These are joined
    # using `AND`
    wheres: t.List[str] = []

    def as_str(self, start: datetime, end: datetime, dialect: str = "bigquery") -> str:
        where_expressions: t.List[exp.Expression] = []
        if self.time_column:
            where_expressions.append(
                exp.GTE(
                    this=exp.to_column(self.time_column),
                    expression=exp.Literal(
                        this=start.strftime("%Y-%m-%d"), is_string=True
                    ),
                )
            )
            where_expressions.append(
                exp.LT(
                    this=exp.to_column(self.time_column),
                    expression=exp.Literal(
                        this=end.strftime("%Y-%m-%d"), is_string=True
                    ),
                )
            )
        additional_where_expressions = [parse_one(where) for where in self.wheres]
        where_expressions.extend(additional_where_expressions)
        return " AND ".join([ex.sql(dialect=dialect) for ex in where_expressions])

    def has_restriction(self) -> bool:
        return bool(self.time_column or self.wheres)


class TableMappingDestination(BaseModel):
    row_restriction: RowRestriction = Field(default_factory=lambda: RowRestriction())
    table: str = ""

    def has_restriction(self) -> bool:
        if self.row_restriction:
            return self.row_restriction.has_restriction()
        return False


TableMappingConfig = t.Dict[str, str | TableMappingDestination]


class DestinationLoader(t.Protocol):
    def load_from_bq(
        self,
        start: datetime,
        end: datetime,
        source_name: str,
        destination: TableMappingDestination,
    ): ...

    def drop_all(self): ...

    def drop_non_sources(self): ...

    def sqlmesh(self, extra_args: t.List[str], extra_env: t.Dict[str, str]): ...


class BaseLoaderConfig(BaseModel):
    @contextmanager
    def loader(
        self, config: "Config", bq_client: bigquery.Client
    ) -> t.Iterator[DestinationLoader]: ...


class DuckDbLoaderConfig(BaseLoaderConfig):
    type: t.Literal["duckdb"] = "duckdb"
    duckdb_path: str

    def duckdb_connect(self):
        return duckdb.connect(self.duckdb_path)

    @contextmanager
    def loader(self, config: "Config", bq_client: bigquery.Client):
        from metrics_tools.local.loader import DuckDbDestinationLoader

        duckdb_conn = self.duckdb_connect()
        try:
            yield DuckDbDestinationLoader(config, bq_client, duckdb_conn)
        finally:
            duckdb_conn.close()


class LocalTrinoLoaderConfig(BaseLoaderConfig):
    type: t.Literal["local-trino"] = "local-trino"
    duckdb_path: str

    postgres_db: str = "postgres"
    postgres_user: str = "postgres"
    postgres_password: str = "password"

    postgres_k8s_service: str = "trino-psql-postgresql"
    postgres_k8s_namespace: str = "local-trino-psql"
    postgres_k8s_port: int = 5432

    trino_k8s_service: str = "local-trino-trino"
    trino_k8s_namespace: str = "local-trino"
    trino_k8s_port: int = 8080

    minio_k8s_service: str = "minio"
    minio_k8s_namespace: str = "local-minio"
    minio_k8s_port: int = 443
    minio_access_key: str = "admin"
    minio_secret_key: str = "password"

    nessie_k8s_service: str = "nessie"
    nessie_k8s_namespace: str = "local-nessie"
    nessie_k8s_port: int = 19120

    def postgres_connection_string(self, port: int) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@localhost:{port}/{self.postgres_db}"

    def postgres_connect(self, port: int):
        return psycopg2.connect(
            database=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
            host="localhost",
            port=port,
        )

    def minio_client(self, port: int):
        from minio import Minio

        return Minio(
            endpoint=f"localhost:{port}",
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=True,
            cert_check=False,
        )

    def duckdb_connect(self):
        return duckdb.connect(self.duckdb_path)

    def iceberg_catalog(self, minio_port: int, nessie_port: int):
        from pyiceberg.catalog import load_catalog

        return load_catalog(
            "nessie",
            **{
                "uri": f"http://localhost:{nessie_port}/iceberg",
                "s3.endpoint": f"https://127.0.0.1:{minio_port}",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
            },
        )

    @contextmanager
    def loader(self, config: "Config", bq_client: bigquery.Client):
        from kr8s.objects import Service
        from kr8s.portforward import PortForward
        from metrics_tools.local.loader import LocalTrinoDestinationLoader

        minio_service = t.cast(
            Service,
            Service.get(
                name=self.minio_k8s_service, namespace=self.minio_k8s_namespace
            ),
        )

        nessie_service = t.cast(
            Service,
            Service.get(
                name=self.nessie_k8s_service, namespace=self.nessie_k8s_namespace
            ),
        )

        minio_forward_context = minio_service.portforward(
            remote_port=self.minio_k8s_port
        )
        assert isinstance(minio_forward_context, PortForward)

        nessie_forward_context = nessie_service.portforward(
            remote_port=self.nessie_k8s_port
        )

        with nessie_forward_context as nessie_port:  # type: ignore
            logger.debug(f"Proxied nessie to port: {nessie_port}")
            with minio_forward_context as minio_port:  # type: ignore
                logger.debug(f"proxied minio to port: {minio_port}")
                duckdb_conn = self.duckdb_connect()
                loader = LocalTrinoDestinationLoader(
                    config,
                    bq_client,
                    duckdb_conn,
                    self.minio_client(minio_port),
                    self.iceberg_catalog(minio_port, nessie_port),
                    f"https://localhost:{minio_port}",
                )
                try:
                    yield loader
                finally:
                    duckdb_conn.close()


LoaderTypes = t.Union[DuckDbLoaderConfig, LocalTrinoLoaderConfig]


class LoaderConfig(BaseModel):
    type: str
    config: LoaderTypes = Field(discriminator="type")


class Config(BaseModel):
    table_mapping: TableMappingConfig
    repo_dir: str
    max_days: int = 7
    max_results_per_query: int = 0
    project_id: str = "opensource-observer"
    timeseries_start: str = "2024-12-01"
    loader: LoaderConfig

    def loader_instance(self):
        return self.loader.config.loader(self, bigquery.Client(project=PROJECT_ID))
