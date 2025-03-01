from datetime import datetime, timedelta
from typing import List, Literal, Optional

import dlt
from dagster import AssetExecutionContext, ResourceParam, WeeklyPartitionsDefinition
from dlt.destinations.adapters import bigquery_adapter
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from oso_dagster.config import DagsterConfig
from oso_dagster.factories import dlt_factory, pydantic_to_dlt_nullable_columns
from oso_dagster.utils.secrets import secret_ref_arg
from pydantic import UUID4, BaseModel, Field

from ..utils.common import QueryArguments, QueryConfig, query_with_retry


class Host(BaseModel):
    id: UUID4
    type: str
    slug: str
    name: Optional[str] = None
    legalName: Optional[str] = None
    description: Optional[str] = None
    currency: Optional[str] = None


class Amount(BaseModel):
    value: float
    currency: str
    valueInCents: float


class TaxInfo(BaseModel):
    id: str
    type: str
    rate: float
    idNumber: str


class Location(BaseModel):
    id: str
    name: str
    address: str
    country: str
    lat: float
    long: float


class SocialLink(BaseModel):
    type: str
    url: str
    createdAt: datetime
    updatedAt: datetime


class Account(BaseModel):
    id: str
    slug: str
    type: str
    name: Optional[str] = None
    legalName: Optional[str] = None
    description: Optional[str] = None
    longDescription: Optional[str] = None
    tags: Optional[List[str]] = None
    socialLinks: Optional[List[SocialLink]] = None
    expensePolicy: Optional[str] = None
    isIncognito: Optional[bool] = None
    imageUrl: Optional[str] = None
    backgroundImageUrl: Optional[str] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None
    isArchived: Optional[bool] = None
    isFrozen: Optional[bool] = None
    isAdmin: Optional[bool] = None
    isHost: Optional[bool] = None
    emails: Optional[List[str]] = None
    location: Optional[Location] = None


class PaymentMethod(BaseModel):
    id: str
    type: str
    name: Optional[str] = None
    data: Optional[str] = None
    balance: Optional[Amount] = None
    account: Optional[Account] = None


class PayoutMethod(BaseModel):
    id: str
    type: str
    name: Optional[str] = None
    data: Optional[str] = None


class VirtualCard(BaseModel):
    id: str
    account: Optional[Account] = None
    name: Optional[str] = None
    last4: Optional[str] = None
    status: Optional[str] = None
    currency: Optional[str] = None
    provider: Optional[str] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None


class Item(BaseModel):
    id: str
    amountV2: Optional[Amount] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None
    incurredAt: Optional[datetime] = None
    description: Optional[str] = None
    url: Optional[str] = None


class Expense(BaseModel):
    id: str
    legacyId: int
    description: str
    longDescription: Optional[str] = None
    reference: Optional[str] = None
    taxes: Optional[TaxInfo] = None
    createdAt: Optional[datetime] = None
    currency: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None
    approvedBy: Optional[Account] = None
    paidBy: Optional[Account] = None
    onHold: Optional[bool] = None
    account: Optional[Account] = None
    payee: Optional[Account] = None
    payeeLocation: Optional[Location] = None
    createdByAccount: Optional[Account] = None
    host: Optional[Host] = None
    payoutMethod: Optional[PayoutMethod] = None
    paymentMethod: Optional[PaymentMethod] = None
    virtualCard: Optional[VirtualCard] = None
    items: Optional[List[Item]] = None
    invoiceInfo: Optional[str] = None
    merchantId: Optional[str] = None
    requestedByAccount: Optional[Account] = None
    requiredLegalDocuments: Optional[List[str]] = None


class Order(BaseModel):
    id: str
    legacyId: int
    description: Optional[str] = None
    amount: Optional[Amount] = None
    taxAmount: Optional[Amount] = None
    totalAmount: Optional[Amount] = None
    quantity: Optional[int] = None
    status: Optional[str] = None


class Transaction(BaseModel):
    id: UUID4
    legacyId: int
    group: UUID4
    type: str
    kind: Optional[str] = None
    description: str
    amount: Optional[Amount] = None
    amountInHostCurrency: Optional[Amount] = None
    hostCurrencyFxRate: Optional[float] = None
    netAmount: Optional[Amount] = None
    netAmountInHostCurrency: Optional[Amount] = None
    taxAmount: Optional[Amount] = None
    taxInfo: Optional[TaxInfo] = None
    platformFee: Optional[Amount] = None
    hostFee: Optional[Amount] = None
    paymentProcessorFee: Optional[Amount] = None
    account: Optional[Account] = None
    fromAccount: Optional[Account] = None
    toAccount: Optional[Account] = None
    expense: Optional[Expense] = None
    order: Optional[Order] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None
    isRefunded: Optional[bool] = None
    isRefund: Optional[bool] = None
    isDisputed: Optional[bool] = None
    isInReview: Optional[bool] = None
    paymentMethod: Optional[PaymentMethod] = None
    payoutMethod: Optional[PayoutMethod] = None
    isOrderRejected: bool
    merchantId: Optional[UUID4] = None
    invoiceTemplate: Optional[str] = None
    host: Optional[Host] = None


# The first transaction on Open Collective was on January 23, 2015
OPEN_COLLECTIVE_TX_EPOCH = "2015-01-23T05:00:00.000Z"

# The maximum is 1000 nodes per page, we will retry until a minimum of 100 nodes per page is reached
OPEN_COLLECTIVE_MAX_NODES_PER_PAGE = 1000

# The minimum number of nodes per page, if this threshold is reached, the query will fail
OPEN_COLLECTIVE_MIN_NODES_PER_PAGE = 100

# The rate limit wait time in seconds
OPEN_COLLECTIVE_RATELIMIT_WAIT_SECONDS = 65

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


class OpenCollectiveParameters(QueryArguments):
    limit: int = Field(
        ...,
        gt=0,
        description="Number of nodes to fetch per query.",
    )
    type: str
    dateFrom: str
    dateTo: str


def open_collective_graphql_amount(key: str):
    """Returns a GraphQL query string for amount information."""
    return f"""
    {key} {{
        value
        currency
        valueInCents
    }}
    """


def open_collective_graphql_tax_info(key: str):
    """Returns a GraphQL query string for tax information."""
    return f"""
    {key} {{
        id
        type
        rate
        idNumber
    }}
    """


def open_collective_graphql_location(key: str):
    """Returns a GraphQL query string for location information."""
    return f"""
    {key} {{
        id
        name
        address
        country
        lat
        long
    }}
    """


def open_collective_graphql_host(key: str):
    """Returns a GraphQL query string for host information."""
    return f"""
    {key} {{
        id
        type
        slug
        name
        legalName
        description
        currency
    }}
    """


def open_collective_graphql_payment_method(key: str):
    """Returns a GraphQL query string for payment method information."""
    return f"""
    {key} {{
        id
        type
        {open_collective_graphql_amount("balance")}
        {open_collective_graphql_account("account")}
    }}
    """


def open_collective_graphql_account(key: str):
    """Returns a GraphQL query string for account information."""
    return f"""
    {key} {{
        id
        slug
        type
        name
        legalName
        description
        longDescription
        tags
        socialLinks {{
            type
            url
            createdAt
            updatedAt
        }}
        expensePolicy
        isIncognito
        imageUrl
        backgroundImageUrl
        createdAt
        updatedAt
        isArchived
        isFrozen
        isAdmin
        isHost
        isAdmin
        emails
        {open_collective_graphql_location("location")}
    }}
    """


def get_open_collective_data(
    context: AssetExecutionContext,
    client: Client,
    kind: Literal["DEBIT", "CREDIT"],
    date_from: str,
    date_to: str,
):
    """
    Retrieves Open Collective data using the provided client and query parameters.

    Args:
        context (AssetExecutionContext): The execution context of the asset.
        client (Client): The client object used to execute the GraphQL queries.
        kind (str): The transaction type. Either "DEBIT" or "CREDIT".
        date_from (str): The start date for the query.
        date_to (str): The end date for the query.

    Yields:
        list: A list of transaction nodes retrieved from Open Collective.

    Returns:
        list: An empty list if an exception occurs during the query execution.
    """

    total_query = gql(
        """
        query ($type: TransactionType, $dateFrom: DateTime!, $dateTo: DateTime!) {
          transactions(
            type: $type
            dateFrom: $dateFrom
            dateTo: $dateTo
          ) {
            totalCount
          }
        }
        """
    )

    total = client.execute(
        total_query,
        variable_values={
            "type": kind,
            "dateFrom": date_from,
            "dateTo": date_to,
        },
    )

    expense_query = gql(
        f"""
    query (
        $limit: Int!,
        $offset: Int!,
        $type: TransactionType!,
        $dateFrom: DateTime!,
        $dateTo: DateTime!
    ) {{
        transactions(
            limit: $limit
            offset: $offset
            type: $type
            dateFrom: $dateFrom
            dateTo: $dateTo
        ) {{
            totalCount
            nodes {{
                id
                legacyId
                group
                type
                kind
                description
                {open_collective_graphql_amount("amount")}
                {open_collective_graphql_amount("amountInHostCurrency")}
                hostCurrencyFxRate
                {open_collective_graphql_amount("netAmount")}
                {open_collective_graphql_amount("netAmountInHostCurrency")}
                {open_collective_graphql_amount("taxAmount")}
                {open_collective_graphql_tax_info("taxInfo")}
                {open_collective_graphql_amount("platformFee")}
                {open_collective_graphql_amount("hostFee")}
                {open_collective_graphql_amount("paymentProcessorFee")}
                {open_collective_graphql_account("account")}
                {open_collective_graphql_account("fromAccount")}
                {open_collective_graphql_account("toAccount")}
                expense {{
                    id
                    legacyId
                    description
                    longDescription
                    reference
                    {open_collective_graphql_tax_info("taxes")}
                    createdAt
                    currency
                    type
                    status
                    {open_collective_graphql_account("approvedBy")}
                    {open_collective_graphql_account("paidBy")}
                    onHold
                    {open_collective_graphql_account("account")}
                    {open_collective_graphql_account("payee")}
                    {open_collective_graphql_location("payeeLocation")}
                    {open_collective_graphql_account("createdByAccount")}
                    {open_collective_graphql_host("host")}
                    payoutMethod {{
                        id
                        type
                        name
                        isSaved
                        data
                    }}
                    {open_collective_graphql_payment_method("paymentMethod")}
                    virtualCard {{
                        id
                        {open_collective_graphql_account("account")}
                        name
                        last4
                        status
                        currency
                        provider
                        createdAt
                        updatedAt
                    }}
                    items {{
                        id
                        {open_collective_graphql_amount("amountV2")}
                        createdAt
                        updatedAt
                        incurredAt
                        description
                        url
                    }}
                    invoiceInfo
                    merchantId
                    {open_collective_graphql_account("requestedByAccount")}
                    requiredLegalDocuments
                }}
                order {{
                    id
                    legacyId
                    description
                    {open_collective_graphql_amount("amount")}
                    {open_collective_graphql_amount("taxAmount")}
                    {open_collective_graphql_amount("totalAmount")}
                    quantity
                    status
                }}
                createdAt
                updatedAt
                isRefunded
                isRefund
                isDisputed
                isInReview
                {open_collective_graphql_payment_method("paymentMethod")}
                payoutMethod {{
                    id
                    type
                    name
                    data
                }}
                isOrderRejected
                merchantId
                invoiceTemplate
                {open_collective_graphql_host("host")}
                }}
            }}
        }}
        """
    )

    total_count = total["transactions"]["totalCount"]

    context.log.info(f"Total count of transactions: {total_count}")

    config = QueryConfig(
        limit=OPEN_COLLECTIVE_MAX_NODES_PER_PAGE,
        minimum_limit=OPEN_COLLECTIVE_MIN_NODES_PER_PAGE,
        query_arguments=OpenCollectiveParameters(
            limit=OPEN_COLLECTIVE_MAX_NODES_PER_PAGE,
            offset=0,
            type=kind,
            dateFrom=date_from,
            dateTo=date_to,
        ),
        step_key="offset",
        extract_fn=lambda query: query["transactions"]["nodes"],
        ratelimit_wait_seconds=OPEN_COLLECTIVE_RATELIMIT_WAIT_SECONDS,
    )

    yield from query_with_retry(
        client,
        context,
        expense_query,
        total_count,
        config,
    )


def get_open_collective_expenses(
    context: AssetExecutionContext,
    client: Client,
    kind: Literal["DEBIT", "CREDIT"],
):
    """
    Get open collective expenses.

    Args:
        context (AssetExecutionContext): The asset execution context.
        client (Client): The client object.
        kind (str): The kind of expenses. Either "DEBIT" or "CREDIT".

    Yields:
        Generator: A generator that yields open collective data.
    """

    start = datetime.strptime(context.partition_key, "%Y-%m-%d")
    end = start + timedelta(weeks=1)

    start_date = f"{start.isoformat().split(".")[0]}Z"
    end_date = f"{end.isoformat().split(".")[0]}Z"

    yield from get_open_collective_data(context, client, kind, start_date, end_date)


def base_open_collective_client(personal_token: str):
    """
    Creates and returns a client for interacting with the Open Collective API.

    Args:
        personal_token (str): The personal token used for authentication.

    Returns:
        Client: The Open Collective client.
    """

    transport = RequestsHTTPTransport(
        url="https://api.opencollective.com/graphql/v2",
        use_json=True,
        headers={
            "Personal-Token": personal_token,
        },
    )

    client = Client(
        transport=transport,
        fetch_schema_from_transport=True,
    )

    return client


@dlt_factory(
    key_prefix="open_collective",
    partitions_def=WeeklyPartitionsDefinition(
        start_date=OPEN_COLLECTIVE_TX_EPOCH.split("T", maxsplit=1)[0],
        end_offset=1,
    ),
    op_tags={
        "dagster-k8s/config": K8S_CONFIG,
    },
)
def expenses(
    context: AssetExecutionContext,
    global_config: ResourceParam[DagsterConfig],
    personal_token: str = secret_ref_arg(
        group_name="open_collective", key="personal_token"
    ),
):
    """
    Create and register a Dagster asset that materializes Open Collective expenses.

    Args:
        context (AssetExecutionContext): The execution context of the asset.
        personal_token (str): The personal token for authentication.

    Yields:
        Generator: A generator that yields Open Collective expenses.
    """

    client = base_open_collective_client(personal_token)
    resource = dlt.resource(
        get_open_collective_expenses(context, client, "DEBIT"),
        name="expenses",
        columns=pydantic_to_dlt_nullable_columns(Transaction),
        primary_key="id",
        write_disposition="merge",
    )

    if global_config.enable_bigquery:
        bigquery_adapter(
            resource,
            partition="created_at",
        )

    yield resource


@dlt_factory(
    key_prefix="open_collective",
    partitions_def=WeeklyPartitionsDefinition(
        start_date=OPEN_COLLECTIVE_TX_EPOCH.split("T", maxsplit=1)[0],
        end_offset=1,
    ),
    op_tags={
        "dagster-k8s/config": K8S_CONFIG,
    },
)
def deposits(
    global_config: ResourceParam[DagsterConfig],
    context: AssetExecutionContext,
    personal_token: str = secret_ref_arg(
        group_name="open_collective", key="personal_token"
    ),
):
    """
    Create and register a Dagster asset that materializes Open Collective deposits.

    Args:
        context (AssetExecutionContext): The execution context of the asset.
        personal_token (str): The personal token for authentication.

    Yields:
        Generator: A generator that yields Open Collective deposits.
    """

    client = base_open_collective_client(personal_token)
    resource = dlt.resource(
        get_open_collective_expenses(context, client, "CREDIT"),
        name="deposits",
        columns=pydantic_to_dlt_nullable_columns(Transaction),
        primary_key="id",
        write_disposition="merge",
    )

    if global_config.enable_bigquery:
        bigquery_adapter(
            resource,
            partition="created_at",
        )

    yield resource
