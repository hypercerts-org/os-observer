import _ from "lodash";

// Not ideal, but this was a hack to get the data type out of the sql by hand
// easily. Will refactor on a subsequent change.
function define<T>(sql: string, rowType: T): { sql: string; rowType: T } {
  return {
    sql: sql,
    rowType: rowType,
  };
}

const artifactResponse = {
  artifact_id: "",
  artifact_source: "",
  artifact_namespace: "",
  artifact_name: "",
  artifact_url: "",
};

const GET_ALL_ARTIFACTS = define(
  `
  SELECT 
    artifact_id, 
    artifact_source, 
    artifact_namespace,
    artifact_name,
    artifact_url
  FROM artifacts_v1;
`,
  artifactResponse,
);

const GET_ARTIFACTS_BY_IDS = define(
  `
  SELECT
    artifact_id, 
    artifact_source, 
    artifact_namespace,
    artifact_name,
    artifact_url
  FROM artifacts_v1
  WHERE 
    artifact_id in {artifactIds: Array(String)};
`,
  artifactResponse,
);

const GET_ARTIFACT_BY_NAME = define(
  `
  SELECT
    artifact_id, 
    artifact_source, 
    artifact_namespace,
    artifact_name,
    artifact_url
  FROM artifacts_v1
  WHERE 
    artifact_source = {artifactSource: String}
    and artifact_namespace = {artifactNamespace: String}
    and artifact_name = {artifactName: String};
`,
  artifactResponse,
);

const GET_ARTIFACT_IDS_BY_PROJECT_IDS = define(
  `
  SELECT
    artifact_id
  FROM artifacts_by_project_v1
  WHERE
    project_id = {projectId: String};
`,
  { artifact_id: "" },
);

const projectResponse = {
  project_id: "",
  project_source: "",
  project_namespace: "",
  project_name: "",
  display_name: "",
};

const projectResponseWithDescription = _.merge(projectResponse, {
  description: "",
});

const GET_ALL_PROJECTS = define(
  `
  SELECT
    project_id,
    project_source,
    project_namespace,
    project_name,
    display_name
  FROM projects_v1;
`,
  projectResponse,
);

const GET_PROJECTS_BY_IDS = define(
  `
  SELECT
    project_id,
    project_source,
    project_namespace,
    project_name,
    display_name,
    description
  FROM projects_v1
  WHERE 
    project_id in {projectIds: Array(String)};
`,
  projectResponseWithDescription,
);

const GET_PROJECT_BY_NAME = define(
  `
  SELECT
    project_id,
    project_source,
    project_namespace,
    project_name,
    display_name,
    description
  FROM projects_v1
  WHERE
    project_source = {projectSource: String}
    AND project_namespace = {projectNamespace: String}
    AND project_name = {projectName: String};
`,
  projectResponseWithDescription,
);

const GET_PROJECT_IDS_BY_COLLECTION_NAME = define(
  `
  SELECT
    project_id,
  FROM projects_by_collection_v1
  WHERE
    collection_source = {collectionSource: String}
    AND collection_namespace = {collectionNamespace: String}
    AND collection_name = {collectionName: String};
  `,
  { project_id: "" },
);

const collectionResponse = {
  collection_id: "",
  collection_source: "",
  collection_namespace: "",
  collection_name: "",
  display_name: "",
};

const collectionResponseWithDescription = _.merge(collectionResponse, {
  description: "",
});

const GET_ALL_COLLECTIONS = define(
  `
  SELECT 
    collection_id, 
    collection_source, 
    collection_namespace, 
    collection_name, 
    display_name
  FROM 
    collections_v1;
`,
  collectionResponse,
);

const GET_COLLECTIONS_BY_IDS = define(
  `
  SELECT 
    collection_id, 
    collection_source, 
    collection_namespace, 
    collection_name, 
    display_name, 
    description
  FROM 
    collections_v1
  WHERE 
    collection_id IN {collectionIds: Array(String)};
`,
  collectionResponseWithDescription,
);

const GET_COLLECTION_BY_NAME = define(
  `
  SELECT 
    collection_id, 
    collection_source, 
    collection_namespace, 
    collection_name, 
    display_name, 
    description
  FROM 
    collections_v1
  WHERE 
    collection_source = {collectionSource: String}
    AND collection_namespace = {collectionNamespace: String}
    AND collection_name = {collectionName: String};
`,
  collectionResponseWithDescription,
);

const GET_COLLECTION_IDS_BY_PROJECT_IDS = define(
  `
  SELECT 
    collection_id
  FROM 
    projects_by_collection_v1
  WHERE 
    project_id IN {projectIds: Array(String)};
`,
  { collection_id: "" },
);

/**********************
 * METRICS
 **********************/

const metricResponse = {
  metric_id: "",
  metric_source: "",
  metric_namespace: "",
  metric_name: "",
  display_name: "",
};

const metricResponseWithDescription = _.merge(metricResponse, {
  description: "",
});

const GET_ALL_METRICS = define(
  `
  SELECT 
    metric_id, 
    metric_source, 
    metric_namespace, 
    metric_name, 
    display_name, 
    description
  FROM 
    metrics_v0;
  `,
  metricResponseWithDescription,
);

const GET_METRICS_BY_IDS = define(
  `
  SELECT 
    metric_id, 
    metric_source, 
    metric_namespace, 
    metric_name, 
    display_name, 
    description
  FROM 
    metrics_v0
  WHERE 
    metric_id IN {metricIds: Array(String)};
`,
  metricResponseWithDescription,
);

const GET_METRIC_BY_NAME = define(
  `
  SELECT 
    metric_id, 
    metric_source, 
    metric_namespace, 
    metric_name, 
    display_name, 
    description
  FROM 
    metrics_v0
  WHERE 
    metric_source = {metricSource: String}
    AND metric_namespace = {metricNamespace: String}
    AND metric_name = {metricName: String};
`,
  metricResponseWithDescription,
);

const GET_KEY_METRICS_BY_ARTIFACT = define(
  `
  SELECT
    artifact_id,
    metric_id,
    sample_date,
    amount,
    unit
  FROM key_metrics_by_artifact_v0
  WHERE 
    artifact_id IN {artifactIds: Array(String)}
    AND metric_id IN {metricIds: Array(String)};
  `,
  {
    artifact_id: "",
    metric_id: "",
    sample_date: "",
    amount: 0,
    unit: "",
  },
);

const GET_KEY_METRICS_BY_PROJECT = define(
  `
  SELECT
    project_id,
    metric_id,
    sample_date,
    amount,
    unit
  FROM key_metrics_by_project_v0
  WHERE 
    project_id IN {projectIds: Array(String)}
    AND metric_id IN {metricIds: Array(String)};
  `,
  {
    project_id: "",
    metric_id: "",
    sample_date: "",
    amount: 0,
    unit: "",
  },
);

const GET_KEY_METRICS_BY_COLLECTION = define(
  `
  SELECT
    collection_id,
    metric_id,
    sample_date,
    amount,
    unit
  FROM key_metrics_by_collection_v0
  WHERE 
    collection_id IN {collectionIds: Array(String)}
    AND metric_id IN {metricIds: Array(String)};
  `,
  {
    collection_id: "",
    metric_id: "",
    sample_date: "",
    amount: 0,
    unit: "",
  },
);

/**********************
 * EVENTS
 **********************/

const GET_ALL_EVENT_TYPES = define(
  `
  SELECT 
    event_type
  FROM 
    event_types_v1;
`,
  { event_type: "" },
);

export {
  GET_ALL_ARTIFACTS,
  GET_ARTIFACTS_BY_IDS,
  GET_ARTIFACT_BY_NAME,
  GET_ARTIFACT_IDS_BY_PROJECT_IDS,
  GET_ALL_PROJECTS,
  GET_PROJECTS_BY_IDS,
  GET_PROJECT_BY_NAME,
  GET_PROJECT_IDS_BY_COLLECTION_NAME,
  GET_ALL_COLLECTIONS,
  GET_COLLECTIONS_BY_IDS,
  GET_COLLECTION_BY_NAME,
  GET_COLLECTION_IDS_BY_PROJECT_IDS,
  GET_ALL_METRICS,
  GET_METRICS_BY_IDS,
  GET_METRIC_BY_NAME,
  GET_KEY_METRICS_BY_ARTIFACT,
  GET_KEY_METRICS_BY_PROJECT,
  GET_KEY_METRICS_BY_COLLECTION,
  GET_ALL_EVENT_TYPES,
};
