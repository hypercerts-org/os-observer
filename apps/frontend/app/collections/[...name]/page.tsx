import { notFound } from "next/navigation";
import { cache } from "react";
import _ from "lodash";
import { PlasmicComponent } from "@plasmicapp/loader-nextjs";
import { PLASMIC } from "../../../plasmic-init";
import { PlasmicClientRootProvider } from "../../../plasmic-init-client";
import {
  cachedGetCollectionByName,
  cachedGetProjectsByIds,
  cachedGetMetricsByIds,
  cachedGetKeyMetricsByProject,
  cachedGetProjectIdsByCollectionName,
} from "../../../lib/clickhouse/cached-queries";
import { COLLECTION_PAGE_PROJECT_METRIC_IDS } from "../../../lib/clickhouse/metrics-config";
import { logger } from "../../../lib/logger";
import { catchallPathToString } from "../../../lib/paths";

const COLLECTION_SOURCE = "OSS_DIRECTORY";
const COLLECTION_NAMESPACE = "oso";
const PLASMIC_COMPONENT = "CollectionPage";
//export const dynamic = STATIC_EXPORT ? "force-static" : "force-dynamic";
//export const dynamic = "force-static";
export const dynamic = "force-dynamic";
export const dynamicParams = true;
export const revalidate = false; // 3600 = 1 hour
// TODO: This cannot be empty due to this bug
// https://github.com/vercel/next.js/issues/61213
const STATIC_EXPORT_SLUGS: string[] = ["optimism"];
export async function generateStaticParams() {
  return STATIC_EXPORT_SLUGS.map((s) => ({
    name: [s],
  }));
}

const cachedFetchComponent = cache(async (componentName: string) => {
  try {
    const plasmicData = await PLASMIC.fetchComponentData(componentName);
    return plasmicData;
  } catch (e) {
    logger.warn(e);
    return null;
  }
});

/**
 * This SSR route allows us to fetch the project from the database
 * on the first HTTP request, which should be faster than fetching it client-side
 */

type CollectionPagePath = {
  name: string[];
};

type CollectionPageProps = {
  params: CollectionPagePath;
};

export default async function CollectionPage(props: CollectionPageProps) {
  const { params } = props;
  if (!params.name || !Array.isArray(params.name) || params.name.length < 1) {
    logger.warn("Invalid collection page path", params);
    notFound();
  }

  // Get project metadata from the database
  const name = catchallPathToString(params.name);
  const data1 = await Promise.all([
    await cachedGetCollectionByName({
      collectionSource: COLLECTION_SOURCE,
      collectionNamespace: COLLECTION_NAMESPACE,
      collectionName: name,
    }),
    await cachedGetProjectIdsByCollectionName({
      collectionSource: COLLECTION_SOURCE,
      collectionNamespace: COLLECTION_NAMESPACE,
      collectionName: name,
    }),
  ]);
  const collectionArray = data1[0];
  const projectIdArray = data1[1];
  if (
    !Array.isArray(collectionArray) ||
    collectionArray.length < 1 ||
    !Array.isArray(projectIdArray)
  ) {
    logger.warn(`Cannot find collection (name=${name})`);
    notFound();
  }
  const collection = collectionArray[0];
  //console.log("project", project);

  // Parallelize getting things related to the project
  const projectIds = projectIdArray.map((x) => x.project_id);
  const metricIds = [...COLLECTION_PAGE_PROJECT_METRIC_IDS];
  const data2 = await Promise.all([
    cachedGetProjectsByIds({ projectIds }),
    cachedGetMetricsByIds({ metricIds }),
    cachedGetKeyMetricsByProject({
      projectIds,
      metricIds,
    }),
  ]);
  const projects = data2[0];
  const projectsMap = _.keyBy(projects, (x) => x.project_id);
  const allMetrics = data2[1];
  const metricsMap = _.keyBy(allMetrics, (x) => x.metric_id);
  const keyMetricsData = data2[2];
  const keyMetrics = keyMetricsData.map((x) => ({
    ...x,
    project_display_name: projectsMap[x.project_id]?.display_name,
    metric_display_name: metricsMap[x.metric_id]?.display_name,
  }));
  const metricsByProjectId = _.groupBy(keyMetrics, (x) => x.project_id);
  const projectsTable = _.values(
    _.mapValues(metricsByProjectId, (x) => {
      return {
        Project: x[0].project_display_name,
        ..._.fromPairs(x.map((y) => [y.metric_display_name, y.amount])),
      };
    }),
  );
  console.log("!!!");
  console.log(collection);
  console.log(projectsTable);

  // Get Plasmic component
  const plasmicData = await cachedFetchComponent(PLASMIC_COMPONENT);
  if (!plasmicData) {
    logger.warn(`Unable to get componentName=${PLASMIC_COMPONENT}`);
    notFound();
  }
  const compMeta = plasmicData.entryCompMetas[0];

  return (
    <PlasmicClientRootProvider
      prefetchedData={plasmicData}
      pageParams={compMeta.params}
    >
      <PlasmicComponent
        component={compMeta.displayName}
        componentProps={{
          metadata: collection,
          projectsTable,
        }}
      />
    </PlasmicClientRootProvider>
  );
}
