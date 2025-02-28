import { notFound } from "next/navigation";
import _ from "lodash";
import { cache } from "react";
import { PlasmicComponent } from "@plasmicapp/loader-nextjs";
import { PLASMIC } from "../../../plasmic-init";
import { PlasmicClientRootProvider } from "../../../plasmic-init-client";
import {
  cachedGetProjectByName,
  cachedGetKeyMetricsByProject,
  cachedGetMetricsByIds,
} from "../../../lib/clickhouse/cached-queries";
import { PROJECT_PAGE_METRIC_IDS } from "../../../lib/clickhouse/metrics-config";
import { logger } from "../../../lib/logger";
import { catchallPathToString } from "../../../lib/paths";

const PROJECT_SOURCE = "OSS_DIRECTORY";
const PROJECT_NAMESPACE = "oso";
const PLASMIC_COMPONENT = "ProjectPage";
//export const dynamic = STATIC_EXPORT ? "force-static" : "force-dynamic";
//export const dynamic = "force-static";
export const dynamic = "force-dynamic";
export const dynamicParams = true;
export const revalidate = false; // 3600 = 1 hour
// TODO: This cannot be empty due to this bug
// https://github.com/vercel/next.js/issues/61213
const STATIC_EXPORT_SLUGS: string[] = ["opensource-observer"];
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

type ProjectPagePath = {
  name: string[];
};

type ProjectPageProps = {
  params: ProjectPagePath;
};

export default async function ProjectPage(props: ProjectPageProps) {
  const { params } = props;
  if (!params.name || !Array.isArray(params.name) || params.name.length < 1) {
    logger.warn("Invalid project page path", params);
    notFound();
  }

  // Get project metadata from the database
  const name = catchallPathToString(params.name);
  const projectArray = await cachedGetProjectByName({
    projectSource: PROJECT_SOURCE,
    projectNamespace: PROJECT_NAMESPACE,
    projectName: name,
  });
  if (!Array.isArray(projectArray) || projectArray.length < 1) {
    logger.warn(`Cannot find project (name=${name})`);
    notFound();
  }
  const project = projectArray[0];
  const projectId = project.project_id;
  //console.log("project", project);

  // Parallelize getting things related to the project
  const metricIds = [...PROJECT_PAGE_METRIC_IDS];
  const data = await Promise.all([
    cachedGetMetricsByIds({ metricIds }),
    cachedGetKeyMetricsByProject({
      projectIds: [projectId],
      metricIds,
    }),
  ]);
  const allMetrics = data[0];
  const metricsMap = _.keyBy(allMetrics, (x) => x.metric_id);
  const keyMetricsData = data[1];
  const keyMetrics = keyMetricsData.map((x) => ({
    ...x,
    display_name: metricsMap[x.metric_id]?.display_name,
    description: metricsMap[x.metric_id]?.description,
  }));
  console.log("!!!");
  console.log(project);
  console.log(keyMetrics);

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
          metadata: project,
          keyMetrics,
        }}
      />
    </PlasmicClientRootProvider>
  );
}
