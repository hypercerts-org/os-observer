import { notFound } from "next/navigation";
import _ from "lodash";
import { cache } from "react";
import { PlasmicComponent } from "@plasmicapp/loader-nextjs";
import { PLASMIC } from "../../../../plasmic-init";
import { PlasmicClientRootProvider } from "../../../../plasmic-init-client";
import {
  cachedGetArtifactByName,
  cachedGetMetricsByIds,
  cachedGetKeyMetricsByArtifact,
} from "../../../../lib/clickhouse/cached-queries";
import { ARTIFACT_PAGE_CODE_METRICS_IDS } from "../../../../lib/clickhouse/metrics-config";
import { logger } from "../../../../lib/logger";

const PLASMIC_COMPONENT = "ArtifactPage";
//export const dynamic = STATIC_EXPORT ? "force-static" : "force-dynamic";
export const dynamic = "force-dynamic";
export const dynamicParams = true;
export const revalidate = false; // 3600 = 1 hour
/**
// TODO: This cannot be empty due to this bug
// https://github.com/vercel/next.js/issues/61213
const STATIC_EXPORT_PARAMS: ArtifactPagePath[] = [
  {
    source: "github",
    name: ["opensource-observer", "oso"],
  },
];
export async function generateStaticParams() {
  return STATIC_EXPORT_PARAMS;
}
*/

async function getDefaultProps() {
  return [];
}

async function getCodeProps(artifactId: string) {
  // Parallelize getting things related to the project
  const metricIds = [...ARTIFACT_PAGE_CODE_METRICS_IDS];
  const data = await Promise.all([
    cachedGetMetricsByIds({ metricIds }),
    cachedGetKeyMetricsByArtifact({
      artifactIds: [artifactId],
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

  return keyMetrics;
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

type ArtifactPagePath = {
  source: string;
  name: string[];
};

type ArtifactPageProps = {
  params: ArtifactPagePath;
};

export default async function ArtifactPage(props: ArtifactPageProps) {
  const { params } = props;

  if (
    !params.source ||
    !params.name ||
    !Array.isArray(params.name) ||
    params.name.length < 1
  ) {
    logger.warn("Invalid artifact page path", params);
    notFound();
  }

  const source = params.source.toUpperCase();
  const [namespace, name] =
    params.name.length > 1 ? params.name : [undefined, params.name[0]];

  // Get artifact metadata from the database
  const artifactArray = await cachedGetArtifactByName({
    artifactSource: source,
    artifactNamespace: namespace ?? "",
    artifactName: name,
  });
  if (!Array.isArray(artifactArray) || artifactArray.length < 1) {
    logger.warn(
      `Cannot find artifact (source=${source}, namespace=${namespace}, name=${name})`,
    );
    notFound();
  }
  const artifact = artifactArray[0];
  const artifactId = artifact.artifact_id;
  const keyMetrics =
    source === "GITHUB"
      ? await getCodeProps(artifactId)
      : await getDefaultProps();
  console.log("!!!");
  console.log(artifact);
  console.log(keyMetrics);

  //console.log(artifact);
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
          metadata: artifact,
          keyMetrics,
        }}
      />
    </PlasmicClientRootProvider>
  );
}
