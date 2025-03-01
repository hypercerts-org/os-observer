import logging

import duckdb
from oso_dagster.assets.defillama import DEFILLAMA_PROTOCOLS, defillama_slug_to_name

from .config import RowRestriction, TableMappingConfig, TableMappingDestination

logger = logging.getLogger(__name__)

DUCKDB_SOURCES_SCHEMA_PREFIX = "sources"

TABLE_MAPPING: TableMappingConfig = {
    "opensource-observer.farcaster.profiles": "bigquery.farcaster.profiles",
    "opensource-observer.farcaster.verifications": "bigquery.farcaster.verifications",
    "opensource-observer.lens_v2_polygon.profile_metadata": "bigquery.lens_v2_polygon.profile_metadata",
    "opensource-observer.lens_v2_polygon.profile_ownership_history": "bigquery.lens_v2_polygon.profile_ownership_history",
    "opensource-observer.op_atlas.project": "bigquery.op_atlas.project",
    "opensource-observer.op_atlas.project__defi_llama_slug": "bigquery.op_atlas.project__defi_llama_slug",
    "opensource-observer.op_atlas.project__farcaster": "bigquery.op_atlas.project__farcaster",
    "opensource-observer.op_atlas.project__website": "bigquery.op_atlas.project__website",
    "opensource-observer.op_atlas.project_contract": "bigquery.op_atlas.project_contract",
    "opensource-observer.op_atlas.project_links": "bigquery.op_atlas.project_links",
    "opensource-observer.op_atlas.project_repository": "bigquery.op_atlas.project_repository",
    "opensource-observer.open_collective.deposits": TableMappingDestination(
        row_restriction=RowRestriction(time_column="created_at"),
        table="bigquery.open_collective.deposits",
    ),
    "opensource-observer.open_collective.expenses": TableMappingDestination(
        row_restriction=RowRestriction(time_column="created_at"),
        table="bigquery.open_collective.expenses",
    ),
    ### TODO start: remove oso_playground dependency
    "opensource-observer.oso_playground.int_events__blockchain": "bigquery.oso.int_events__blockchain",
    "opensource-observer.oso_playground.stg_deps_dev__dependencies": "bigquery.oso.stg_deps_dev__dependencies",
    "opensource-observer.oso_playground.stg_deps_dev__packages": "bigquery.oso.stg_deps_dev__packages",
    "opensource-observer.oso_playground.stg_github__events": "bigquery.oso.stg_github__events",
    ### TODO end
    "opensource-observer.ossd.collections": "bigquery.ossd.collections",
    "opensource-observer.ossd.projects": "bigquery.ossd.projects",
    "opensource-observer.ossd.repositories": "bigquery.ossd.repositories",
    "opensource-observer.ossd.sbom": "bigquery.ossd.sbom",
    # Only grab some data from frax for local testing
    "opensource-observer.optimism_superchain_raw_onchain_data.blocks": TableMappingDestination(
        # row_restriction=f"dt >= '{start_date.strftime("%Y-%m-%d")}' AND dt < '{end_date.strftime("%Y-%m-%d")}' AND chain_id = 252",
        row_restriction=RowRestriction(
            time_column="dt",
            wheres=["chain_id = 252"],
        ),
        table="bigquery.optimism_superchain_raw_onchain_data.blocks",
    ),
    "opensource-observer.optimism_superchain_raw_onchain_data.transactions": TableMappingDestination(
        row_restriction=RowRestriction(
            time_column="dt",
            wheres=["chain_id = 252"],
        ),
        table="bigquery.optimism_superchain_raw_onchain_data.transactions",
    ),
    "opensource-observer.optimism_superchain_raw_onchain_data.traces": TableMappingDestination(
        row_restriction=RowRestriction(
            time_column="dt",
            wheres=["chain_id = 252"],
        ),
        table="bigquery.optimism_superchain_raw_onchain_data.traces",
    ),
    "opensource-observer.optimism_superchain_4337_account_abstraction_data.useroperationevent_logs_v1": TableMappingDestination(
        row_restriction=RowRestriction(
            time_column="dt",
            wheres=["chain_id = 252"],
        ),
        table="bigquery.optimism_superchain_4337_account_abstraction_data.useroperationevent_logs_v1",
    ),
    "opensource-observer.optimism_superchain_4337_account_abstraction_data.enriched_entrypoint_traces_v1": TableMappingDestination(
        row_restriction=RowRestriction(
            time_column="dt",
            wheres=["chain_id = 252"],
        ),
        table="bigquery.optimism_superchain_4337_account_abstraction_data.enriched_entrypoint_traces_v1",
    ),
}

defillama_tables = {
    f"opensource-observer.defillama_tvl.{defillama_slug_to_name(slug)}": f"bigquery.defillama_tvl.{defillama_slug_to_name(slug)}"
    for slug in DEFILLAMA_PROTOCOLS
}
TABLE_MAPPING.update(defillama_tables)


def reset_local_duckdb(path: str):
    conn = duckdb.connect(path)
    response = conn.query("SHOW ALL TABLES")
    schema_names = response.df()["schema"].unique().tolist()
    for schema_name in schema_names:
        if not schema_name.startswith(DUCKDB_SOURCES_SCHEMA_PREFIX):
            logger.info(f"dropping schema {schema_name}")
            conn.query(f"DROP schema {schema_name} cascade")
