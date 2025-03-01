import json
import logging
import os
import uuid
from pathlib import Path
from typing import Dict, List

from dagster import AssetKey
from oso_dagster.config import DagsterConfig

from ..factories import (
    Bq2ClickhouseAssetConfig,
    create_bq2clickhouse_asset,
    early_resources_asset_factory,
)
from ..factories.common import AssetFactoryResponse
from ..utils.bq import BigQueryTableConfig
from ..utils.common import SourceMode

logger = logging.getLogger(__name__)

MART_DIRECTORY = "marts"
INTERMEDIATE_DIRECTORY = "intermediate"
SYNC_KEY = "sync_to_db"


@early_resources_asset_factory()
def all_clickhouse_dbt_mart_assets(
    global_config: DagsterConfig,
) -> AssetFactoryResponse:
    """Deprecated: Creates all Dagster assets from a map of manifests

    Parameters
    ----------
    manifests: Dict[str, Path]
        Result from `load_dbt_manifests()`
        dbt_target => manifest_path

    Returns
    -------
    AssetsFactoryResponse
        a list of Dagster assets
    """

    manifests: Dict[str, Path] = global_config.dbt_manifests

    # We only care about mart models from production
    if global_config.production_dbt_target not in manifests:
        raise Exception(
            f"Expected {global_config.production_dbt_target} in dbt manifests({manifests.keys()})"
        )

    # Load the manifest
    manifest_path = manifests[global_config.production_dbt_target]
    if not os.path.isfile(manifest_path):
        raise Exception(f"{manifest_path} does not exist")
    with open(manifest_path, "r") as f:
        manifest = json.load(f)
        if "nodes" not in manifest:
            raise Exception(f"Expected nodes in {manifest_path}")
        nodes = manifest["nodes"].values()
        # Get manifests of mart models to copy
        marts = list(filter(lambda n: MART_DIRECTORY in n.get("fqn"), nodes))

        # TEMPORARY HACKY CHANGE TO GET INT_EVENTS COPIED TO CLICKHOUSE
        intermediates = list(
            filter(lambda n: INTERMEDIATE_DIRECTORY in n.get("fqn"), nodes)
        )
        marts.extend(intermediates)

        copied_mart_names: List[str] = []
        skipped_mart_names: List[str] = []
        result = AssetFactoryResponse([])
        for n in marts:
            table_name = n.get("name")
            # Only copy marts that are marked for sync
            if n.get("meta").get(SYNC_KEY, False):
                logger.debug(f"Queuing {table_name}")
                copied_mart_names.append(table_name)
                # Create an asset for each mart to copy
                result = result + create_bq2clickhouse_asset(
                    Bq2ClickhouseAssetConfig(
                        key_prefix="clickhouse",
                        asset_name=table_name,
                        deps=[AssetKey(["dbt", "production", table_name])],
                        sync_id=str(uuid.uuid4()),
                        source_config=BigQueryTableConfig(
                            project_id=n.get("database"),
                            dataset_name=n.get("schema"),
                            table_name=table_name,
                            service_account=None,
                        ),
                        staging_bucket=global_config.staging_bucket_url,
                        destination_table_name=table_name,
                        index=n.get("meta").get("index"),
                        tags={"opensource.observer/experimental": "true"},
                        order_by=n.get("meta").get("order_by"),
                        copy_mode=SourceMode.Overwrite,
                        asset_kwargs={
                            "op_tags": {
                                "dagster-k8s/config": {
                                    "merge_behavior": "SHALLOW",
                                    "container_config": {
                                        "resources": {
                                            "requests": {
                                                "cpu": "1000m",
                                                "memory": "1536Mi",
                                            },
                                            "limits": {
                                                "memory": "1536Mi",
                                            },
                                        },
                                    },
                                    "pod_spec_config": {
                                        "node_selector": {
                                            "pool_type": "spot",
                                        },
                                        "tolerations": [
                                            {
                                                "key": "pool_type",
                                                "operator": "Equal",
                                                "value": "spot",
                                                "effect": "NoSchedule",
                                            }
                                        ],
                                    },
                                },
                            }
                        },
                    ),
                )
            # Track which marts were skipped
            else:
                skipped_mart_names.append(table_name)
        logger.debug(
            f"...queued {str(len(copied_mart_names))} marts, skipping {str(len(skipped_mart_names))}"
        )
        return result
