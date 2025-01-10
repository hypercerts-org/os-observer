"""
Auxiliary resources for sqlmesh
"""

import typing as t

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    AssetsDefinition,
    MaterializeResult,
    multi_asset,
)
from dagster_sqlmesh import SQLMeshDagsterTranslator
from dagster_sqlmesh.controller.base import SQLMeshInstance
from metrics_tools.compute.types import TableReference
from metrics_tools.transfer.coordinator import Destination, Source, transfer
from oso_dagster.resources.clickhouse import ClickhouseImporterResource
from oso_dagster.resources.trino import TrinoExporterResource
from sqlglot import exp
from sqlmesh import Context
from sqlmesh.core.dialect import parse_one
from sqlmesh.core.model import Model


class SQLMeshExporter:
    name: str

    def create_export_asset(
        self, mesh: SQLMeshInstance, to_export: t.List[t.Tuple[Model, AssetKey]]
    ) -> AssetsDefinition:
        raise NotImplementedError("Not implemented")


class PrefixedSQLMeshTranslator(SQLMeshDagsterTranslator):
    def __init__(self, prefix: str):
        self._prefix = prefix

    def get_asset_key_fqn(self, context: Context, fqn: str) -> AssetKey:
        key = super().get_asset_key_fqn(context, fqn)
        return key.with_prefix("production").with_prefix("dbt")

    def get_asset_key_from_model(self, context: Context, model: Model) -> AssetKey:
        key = super().get_asset_key_from_model(context, model)
        return key.with_prefix(self._prefix)


class Trino2ClickhouseSQLMeshExporter(SQLMeshExporter):
    def __init__(
        self,
        prefix: str | t.List[str],
        *,
        destination_catalog: str,
        destination_schema: str,
        source_catalog: str,
        source_schema: str,
    ):
        self._prefix = prefix

        self._destination_catalog = destination_catalog
        self._destination_schema = destination_schema

        self._source_catalog = source_catalog
        self._source_schema = source_schema

    def trino_destination_table(self, table_name: str):
        return f"{self._destination_catalog}.{self._destination_schema}.{table_name}"

    def clickhouse_destination_table(self, table_name: str):
        return f"{self._destination_schema}.{table_name}"

    def trino_source_table(self, table_name: str):
        return f"{self._source_catalog}.{self._source_schema}.{table_name}"

    def convert_model_key_to_clickhouse_out(self, key: AssetKey) -> AssetOut:
        return AssetOut(
            key_prefix=self._prefix,
            is_required=False,
        )

    def create_export_asset(
        self, mesh: SQLMeshInstance, to_export: t.List[t.Tuple[Model, AssetKey]]
    ) -> AssetsDefinition:
        clickhouse_outs = {
            key.path[-1]: self.convert_model_key_to_clickhouse_out(key)
            for _, key in to_export
        }
        internal_asset_deps: t.Mapping[str, t.Set[AssetKey]] = {
            key.path[-1]: {key} for _, key in to_export
        }
        deps = [key for _, key in to_export]

        @multi_asset(
            outs=clickhouse_outs,
            internal_asset_deps=internal_asset_deps,
            deps=deps,
            compute_kind="sqlmesh-export",
            can_subset=True,
        )
        async def export(
            context: AssetExecutionContext,
            trino_exporter: TrinoExporterResource,
            clickhouse_importer: ClickhouseImporterResource,
        ):
            async with trino_exporter.get_exporter(
                "trino-export", log_override=context.log
            ) as exporter:
                with clickhouse_importer.get() as importer:
                    selected_output_names = (
                        context.op_execution_context.selected_output_names
                    )
                    for table_name in selected_output_names:
                        await transfer(
                            Source(
                                exporter=exporter,
                                table=TableReference(
                                    catalog_name=self._source_catalog,
                                    schema_name=self._source_schema,
                                    table_name=table_name,
                                ),
                            ),
                            Destination(
                                importer=importer,
                                table=TableReference(
                                    schema_name=self._destination_schema,
                                    table_name=table_name,
                                ),
                            ),
                            log_override=context.log,
                        )
                        yield MaterializeResult(asset_key=AssetKey(table_name))

        return export

    def generate_create_table_query(
        self, model: Model, source_table: str, destination_table: str
    ):
        base_create_query = f"""
            CREATE table {destination_table} (
                placeholder VARCHAR,
            )
        """

        columns = model.columns_to_types
        assert columns is not None, "columns must not be None"

        create_query = parse_one(base_create_query, dialect="trino")
        create_query.this.set(
            "expressions",
            [
                exp.ColumnDef(this=exp.to_identifier(column_name), kind=column_type)
                for column_name, column_type in columns.items()
            ],
        )

        properties = [
            # DO NOT USE exp.EngineProperty. This doesn't currently work
            # properly for sqlglot in this context. Also don't parse a create
            # query that has the WITH (engine = 'something') portion in it. It
            # will parse as an exp.EngineProperty and cause problems when
            # rerendering to sql
            exp.Property(
                this=exp.Var(this="engine"),
                value=exp.Literal(this="MergeTree", is_string=True),
            ),
        ]
        if model.time_column:
            properties.append(
                exp.Property(
                    this=exp.Var(this="order_by"),
                    value=exp.Literal(
                        this=model.time_column.column.sql(dialect="trino"),
                        is_string=True,
                    ),
                )
            )
        create_query.set(
            "properties",
            exp.Properties(
                expressions=properties,
            ),
        )
        return create_query

    def generate_insert_query(
        self, model: Model, source_table: str, destination_table: str
    ) -> exp.Expression:
        assert model.columns_to_types is not None, "columns must not be None"
        select = exp.select(
            *[column_name for column_name in model.columns_to_types.keys()]
        ).from_(source_table)
        insert = exp.Insert(this=exp.to_table(destination_table), expression=select)

        return insert
