from datetime import datetime, timedelta
from typing import Any, Dict

import dlt
from dagster import AssetExecutionContext, WeeklyPartitionsDefinition
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from oso_dagster.factories import dlt_factory, pydantic_to_dlt_nullable_columns
from pydantic import BaseModel, Field

from ..utils.common import QueryArguments, QueryConfig, query_with_retry


class Attestation(BaseModel):
    id: str
    data: str
    decodedDataJson: str
    recipient: str
    attester: str
    time: int
    timeCreated: int
    expirationTime: int
    revocationTime: int
    refUID: str
    revocable: bool
    revoked: bool
    txid: str
    schemaId: str
    ipfsHash: str
    isOffchain: bool


class EASOptimismParameters(QueryArguments):
    take: int = Field(
        ...,
        gt=0,
        description="The number of nodes to fetch per query.",
    )
    skip: int = Field(
        ...,
        ge=0,
        description="The number of nodes to skip.",
    )
    where: Dict[str, Any] = Field(
        ...,
        description="The where clause for the query.",
    )


# The first attestation on EAS Optimism was created on the 07/28/2023 9:22:35 am
EAS_OPTIMISM_FIRST_ATTESTATION = datetime.fromtimestamp(1690557755)

# A sensible limit for the number of nodes to fetch per page
EAS_OPTIMISM_STEP_NODES_PER_PAGE = 10_000

# Minimum limit for the query, after which the query will fail
EAS_OPTIMISM_MINIMUM_LIMIT = 100

# The rate limit wait time in seconds
EAS_OPTIMISM_RATELIMIT_WAIT_SECONDS = 65

# Kubernetes configuration for the asset materialization
K8S_CONFIG = {
    "merge_behavior": "SHALLOW",
    "container_config": {
        "resources": {
            "requests": {"cpu": "2000m", "memory": "3584Mi"},
            "limits": {"memory": "3584Mi"},
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
}


def get_optimism_eas_data(
    context: AssetExecutionContext, client: Client, date_from: float, date_to: float
):
    """
    Retrieves the attestation data from the EAS Optimism GraphQL API.

    Args:
        context (AssetExecutionContext): The asset execution context.
        client (Client): The GraphQL client.
        date_from (float): The start date in timestamp format.
        date_to (float): The end date in timestamp format.

    Yields:
        list: A list of attestation nodes retrieved from EAS Optimism.

    Returns:
        list: An empty list if an exception occurs during the query execution.
    """

    total_query = gql(
        """
        query _count($where: AttestationWhereInput) {
            aggregateAttestation(where: $where) {
                _count {
                    _all
                }
            }
        }        
        """
    )

    total = client.execute(
        total_query,
        variable_values={
            "where": {
                "time": {
                    "gte": date_from,
                    "lte": date_to,
                }
            },
        },
    )

    total_count = total["aggregateAttestation"]["_count"]["_all"]

    context.log.info(f"Total count of attestations: {total_count}")

    attestations_query = gql(
        """
        query Attestations($take: Int, $skip: Int, $where: AttestationWhereInput) {
            attestations(take: $take, skip: $skip, where: $where) {
                id
                data
                decodedDataJson
                recipient
                attester
                time
                timeCreated
                expirationTime
                revocationTime
                refUID
                revocable
                revoked
                txid
                schemaId
                ipfsHash
                isOffchain
            }
        }
        """
    )

    config = QueryConfig(
        limit=EAS_OPTIMISM_STEP_NODES_PER_PAGE,
        minimum_limit=EAS_OPTIMISM_MINIMUM_LIMIT,
        query_arguments=EASOptimismParameters(
            take=EAS_OPTIMISM_STEP_NODES_PER_PAGE,
            offset=0,
            skip=0,
            where={
                "time": {
                    "gte": date_from,
                    "lte": date_to,
                }
            },
        ),
        step_key="skip",
        extract_fn=lambda data: data["attestations"],
        ratelimit_wait_seconds=EAS_OPTIMISM_RATELIMIT_WAIT_SECONDS,
    )

    yield from query_with_retry(
        client,
        context,
        attestations_query,
        total_count,
        config,
    )


def get_optimism_eas(context: AssetExecutionContext, client: Client):
    """
    Get the attestation data from the EAS Optimism GraphQL API.

    Args:
        context (AssetExecutionContext): The asset execution context.
        client (Client): The GraphQL client.

    Yields:
        Generator: A generator that yields the attestation data.
    """

    start = datetime.strptime(context.partition_key, "%Y-%m-%d")
    end = start + timedelta(weeks=1)

    yield from get_optimism_eas_data(
        context, client, start.timestamp(), end.timestamp()
    )


@dlt_factory(
    key_prefix="ethereum_attestation_service_optimism",
    partitions_def=WeeklyPartitionsDefinition(
        start_date=EAS_OPTIMISM_FIRST_ATTESTATION.isoformat().split("T", maxsplit=1)[0],
        end_offset=1,
    ),
    op_tags={
        "dagster-k8s/config": K8S_CONFIG,
    },
)
def attestations(context: AssetExecutionContext):
    """
    Create an asset that retrieves the attestation data from the EAS Optimism GraphQL API.

    Args:
        context (AssetExecutionContext): The asset execution context.

    Yields:
        Asset: The asset that retrieves the attestation data.
    """

    transport = AIOHTTPTransport(url="https://optimism.easscan.org/graphql")
    client = Client(transport=transport, fetch_schema_from_transport=True)

    yield dlt.resource(
        get_optimism_eas(context, client),
        name="attestations",
        columns=pydantic_to_dlt_nullable_columns(Attestation),
        primary_key="id",
        write_disposition="merge",
    )
