// Maintains the same interface as a previous graphql iteration of this

import { cache } from "react";
import {
  GET_ARTIFACTS_BY_IDS,
  GET_ARTIFACT_BY_NAME,
  GET_ARTIFACT_IDS_BY_PROJECT_IDS,
  GET_PROJECTS_BY_IDS,
  GET_PROJECT_BY_NAME,
  GET_PROJECT_IDS_BY_COLLECTION_NAME,
  GET_COLLECTIONS_BY_IDS,
  GET_COLLECTION_BY_NAME,
  GET_COLLECTION_IDS_BY_PROJECT_IDS,
  GET_METRICS_BY_IDS,
  GET_METRIC_BY_NAME,
  GET_KEY_METRICS_BY_ARTIFACT,
  GET_KEY_METRICS_BY_PROJECT,
  GET_KEY_METRICS_BY_COLLECTION,
  GET_ALL_EVENT_TYPES,
} from "./queries";
import { getClickhouseClient } from "../clients/clickhouse";

interface QueryOptions<T> {
  query: {
    sql: string;
    rowType: T;
  };
  variables?: Record<string, any>;
}

async function queryWrapper<T>(opts: QueryOptions<T>): Promise<T[]> {
  const client = getClickhouseClient();
  const rows = await client.query({
    query: opts.query.sql,
    query_params: opts.variables,
  });
  const resultSet = await rows.json();
  return resultSet.data as T[];
}

// Cached getters
const cachedGetAllEventTypes = cache(async () =>
  queryWrapper({
    query: GET_ALL_EVENT_TYPES,
  }),
);

const cachedGetProjectsByIds = cache(
  async (variables: { projectIds: string[] }) =>
    queryWrapper({
      query: GET_PROJECTS_BY_IDS,
      variables,
    }),
);

const cachedGetProjectByName = cache(
  async (variables: {
    projectSource: string;
    projectNamespace: string;
    projectName: string;
  }) =>
    queryWrapper({
      query: GET_PROJECT_BY_NAME,
      variables,
    }),
);

const cachedGetProjectIdsByCollectionName = cache(
  async (variables: {
    collectionSource: string;
    collectionNamespace: string;
    collectionName: string;
  }) =>
    queryWrapper({
      query: GET_PROJECT_IDS_BY_COLLECTION_NAME,
      variables,
    }),
);

const cachedGetArtifactsByIds = cache(
  async (variables: { artifactIds: string[] }) =>
    queryWrapper({
      query: GET_ARTIFACTS_BY_IDS,
      variables,
    }),
);

const cachedGetArtifactByName = cache(
  async (variables: {
    artifactSource: string;
    artifactNamespace: string;
    artifactName: string;
  }) =>
    queryWrapper({
      query: GET_ARTIFACT_BY_NAME,
      variables,
    }),
);

const cachedGetArtifactIdsByProjectIds = cache(
  async (variables: { projectIds: string[] }) =>
    queryWrapper({
      query: GET_ARTIFACT_IDS_BY_PROJECT_IDS,
      variables,
    }),
);

const cachedGetCollectionsByIds = cache(
  async (variables: { collectionIds: string[] }) =>
    queryWrapper({
      query: GET_COLLECTIONS_BY_IDS,
      variables,
    }),
);

const cachedGetCollectionByName = cache(
  async (variables: {
    collectionSource: string;
    collectionNamespace: string;
    collectionName: string;
  }) =>
    queryWrapper({
      query: GET_COLLECTION_BY_NAME,
      variables,
    }),
);

const cachedGetCollectionIdsByProjectIds = cache(
  async (variables: { projectIds: string[] }) =>
    queryWrapper({
      query: GET_COLLECTION_IDS_BY_PROJECT_IDS,
      variables,
    }),
);

const cachedGetMetricsByIds = cache(
  async (variables: { metricIds: string[] }) =>
    queryWrapper({
      query: GET_METRICS_BY_IDS,
      variables,
    }),
);

const cachedGetMetricByName = cache(
  async (variables: {
    metricSource: string;
    metricNamespace: string;
    metricName: string;
  }) =>
    queryWrapper({
      query: GET_METRIC_BY_NAME,
      variables,
    }),
);

const cachedGetKeyMetricsByArtifact = cache(
  async (variables: { metricIds: string[]; artifactIds: string[] }) =>
    queryWrapper({
      query: GET_KEY_METRICS_BY_ARTIFACT,
      variables,
    }),
);

const cachedGetKeyMetricsByProject = cache(
  async (variables: { metricIds: string[]; projectIds: string[] }) =>
    queryWrapper({
      query: GET_KEY_METRICS_BY_PROJECT,
      variables,
    }),
);

const cachedGetKeyMetricsByCollection = cache(
  async (variables: { metricIds: string[]; collectionIds: string[] }) =>
    queryWrapper({
      query: GET_KEY_METRICS_BY_COLLECTION,
      variables,
    }),
);

export {
  cachedGetArtifactsByIds,
  cachedGetArtifactByName,
  cachedGetArtifactIdsByProjectIds,
  cachedGetProjectsByIds,
  cachedGetProjectByName,
  cachedGetProjectIdsByCollectionName,
  cachedGetCollectionByName,
  cachedGetCollectionsByIds,
  cachedGetCollectionIdsByProjectIds,
  cachedGetMetricsByIds,
  cachedGetMetricByName,
  cachedGetKeyMetricsByArtifact,
  cachedGetKeyMetricsByProject,
  cachedGetKeyMetricsByCollection,
  cachedGetAllEventTypes,
};
