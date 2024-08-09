---
title: Use the GraphQL API
sidebar_position: 10
---

The OSO API currently only allows read-only GraphQL queries against a subset
of OSO data (i.e. only mart models like impact metrics, project info).
This API should only be used to fetch data to integrate into a live application in production.
If you want access to the full dataset for data exploration, check out the guides on
[performing queries](./query-data.mdx)
and [Python notebooks](./python-notebooks.md).

## How to Generate an API key

First, go to [www.opensource.observer](https://www.opensource.observer) and create a new account.

If you already have an account, log in. Then create a new personal API key:

1. Go to [Account settings](https://www.opensource.observer/app/settings)
2. In the "API Keys" section, click "+ New"
3. Give your key a label - this is just for you, usually to describe a key's purpose.
4. You should see your brand new key. **Immediately** save this value, as you'll **never** see it again after refreshing the page.
5. Click "Create" to save the key.

**You can create as many keys as you like.**

![generate API key](./generate-api-key.png)

## GraphQL Endpoint

All API requests are sent to the following URL:

```
https://www.opensource.observer/api/v1/graphql
```

You can navigate to our
[public GraphQL explorer](https://www.opensource.observer/graphql)
to explore the schema and execute test queries.

## How to Authenticate

In order to authenticate with the API service, you have to use the `Authorization` HTTP header and `Bearer` authentication on all HTTP requests, like so:

```js
const headers = {
  Authorization: `Bearer ${DEVELOPER_API_KEY}`,
};
```

:::tip
Our API opens a small rate limit for anonymous queries without authentication. Feel free to use for low volume queries.
:::

## Example

This query will fetch the first 10 projects in
[oss-directory](https://github.com/opensource-observer/oss-directory).

```graphql
query GetProjects {
  oso_projectsV1(limit: 10) {
    description
    displayName
    projectId
    projectName
    projectNamespace
    projectSource
  }
}
```

This query will fetch **code metrics** for 10 projects, ordered by `starCount`.

```graphql
query GetCodeMetrics {
  oso_codeMetricsByProjectV1(limit: 10, order_by: [{ starCount: Desc }]) {
    projectId
    projectName
    eventSource
    starCount
    forkCount
    contributorCount
  }
}
```

## GraphQL Explorer

You can navigate to our [public GraphQL explorer](https://www.opensource.observer/graphql) to explore the schema and execute test queries.

![GraphQL explorer](./api-explorer.gif)

:::tip
As shown in the video, you must "Inline Variables" in order for queries to run in the explorer.
:::

The GraphQL schema is automatically generated from [`oso/dbt/models/marts`](https://github.com/opensource-observer/oso/tree/main/dbt/models/marts). Any dbt model defined there will automatically be exported to our GraphQL API. See the guide on [adding DBT models](../contribute/impact-models.md) for more information on contributing to our marts models.

:::warning
Our data pipeline is under heavy development and all table schemas are subject to change until we introduce versioning to marts models.
Please join us on [Discord](https://www.opensource.observer/discord) to stay up to date on updates.
:::

## Rate Limits

All requests are rate limited. There are currently 2 separate rate limits for different resources:

- **anonymous**: Anyone can make a query, even without an authorization token, subject to a low rate limit.
- **developer**: Developers who have been accepted into the [Kariba Data Collective](https://www.kariba.network) and provide an API key in the HTTP header will be subject to a higher rate limit.

:::warning
We are still currently adjusting our rate limits based on capacity and demand. If you feel like your rate limit is too low, please reach out to us on our [Discord](https://www.opensource.observer/discord).
:::
