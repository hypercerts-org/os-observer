import { useQuery } from "@apollo/client";
import dayjs from "dayjs";
import _ from "lodash";
import React from "react";
import {
  assertNever,
  safeCast,
  ensure,
  uncheckedCast,
} from "@opensource-observer/utils";
import {
  GET_TIMESERIES_METRICS_BY_ARTIFACT,
  GET_TIMESERIES_METRICS_BY_PROJECT,
} from "../../lib/graphql/queries";
import { eventTimeToLabel } from "../../lib/parsing";
import { RegistrationProps } from "../../lib/types/plasmic";
import type { EventData } from "../../lib/types/db";
import {
  DataProviderView,
  CommonDataProviderRegistration,
} from "./provider-view";
import type { CommonDataProviderProps } from "./provider-view";
import { useEnsureAuth } from "./apollo-wrapper";

// Types used in the Plasmic registration
type ChartType = "areaChart" | "barList";
type XAxis = "date" | "entity" | "metric";
type EntityType = "artifact" | "project";

// Ideal minimum number of data points in an area chart
type BucketWidth = "day" | "week" | "month";
const MIN_DATA_POINTS = 20;
// Default start time
const DEFAULT_START_DATE = 0;
// Default XAxis if not specified
const DEFAULT_XAXIS: XAxis = "date";

/**
 * Generic query component focused on providing metrics data to visualiation components
 */
type MetricsDataProviderProps = CommonDataProviderProps & {
  chartType: ChartType;
  xAxis?: XAxis; // X-axis
  entityType: EntityType;
  entityIds?: string[];
  metricIds?: string[];
  startDate?: string;
  endDate?: string;
};

/**
 * Plasmic component registration
 */
const MetricsDataProviderRegistration: RegistrationProps<MetricsDataProviderProps> =
  {
    ...CommonDataProviderRegistration,
    chartType: {
      type: "choice",
      helpText: "Pair this with the component in 'children'",
      options: safeCast<ChartType[]>(["areaChart", "barList"]),
    },
    xAxis: {
      type: "choice",
      helpText: "What is the x-axis?",
      options: safeCast<XAxis[]>(["date", "entity", "metric"]),
    },
    entityType: {
      type: "choice",
      helpText: "What kind of entity?",
      options: safeCast<EntityType[]>(["artifact", "project"]),
    },
    entityIds: {
      type: "array",
      defaultValue: [],
    },
    metricIds: {
      type: "array",
      defaultValue: [],
    },
    startDate: {
      type: "string",
      helpText: "YYYY-MM-DD",
    },
    endDate: {
      type: "string",
      helpText: "YYYY-MM-DD",
    },
  };

/**
 * Choose a bucket width based on the number of data points
 */
const getBucketWidth = (props: MetricsDataProviderProps): BucketWidth => {
  const startDate = dayjs(props.startDate ?? DEFAULT_START_DATE);
  const endDate = dayjs(props.endDate);
  if (endDate.diff(startDate, "month") > MIN_DATA_POINTS) {
    return "month";
  } else if (endDate.diff(startDate, "week") > MIN_DATA_POINTS) {
    return "week";
  } else {
    return "day";
  }
};

/**
 * Used in formatting chart data
 */
type FormatOpts = {
  // Should we fill in empty dates with 0's?
  gapFill?: boolean;
};

/**
 * Formats normalized data into inputs to an area chart (for Tremor)
 * @param data
 * @returns
 */
const formatDataToAreaChart = (
  data: EventData[],
  categories: { results: string[]; opts: CategoryOpts },
  formatOpts?: FormatOpts,
) => {
  // Start with an empty data point for each available date
  const emptyDataPoint = _.fromPairs(categories.results.map((c) => [c, 0]));
  const datesWithData = _.uniq(data.map((x) => eventTimeToLabel(x.date)));
  const groupedByDate = _.fromPairs(
    datesWithData.map((d) => [d, _.clone(emptyDataPoint)]),
  );
  //console.log(groupedByDate);

  // Sum the values for each (date, artifactId, eventType)
  data.forEach((d) => {
    const dateLabel = eventTimeToLabel(d.date);
    const category = createCategory(
      d.entityName,
      d.metricName,
      categories.opts,
    );
    groupedByDate[dateLabel][category] += d.amount;
  });
  //console.log(groupedByDate);

  // Flatten into an array
  const unsorted = _.toPairs(groupedByDate).map((x) => ({
    ...x[1],
    date: x[0],
  }));
  //console.log(unsorted);

  // Sort by date
  const sorted = _.sortBy(unsorted, (x) => x.date);
  //console.log(result);

  // Trim empty data at the start and end
  const isEmptyDataPoint = (x: _.Dictionary<number | string>): boolean => {
    for (const cat of categories.results) {
      if (x[cat] !== 0) {
        return false;
      }
    }
    return true;
  };
  let [i, j] = [0, sorted.length - 1];
  while (i < sorted.length && isEmptyDataPoint(sorted[i])) {
    i++;
  }
  while (j > 0 && isEmptyDataPoint(sorted[j])) {
    j--;
  }
  // TODO: we are only trimming the right side to see if users like this better.
  //const sliced = sorted.slice(i, j + 1);
  const sliced = sorted.slice(0, j + 1);
  //categories.results.includes("Downloads") && console.log(sliced);

  // Fill in any empty dates
  let filled;
  if (formatOpts?.gapFill) {
    let currDate = dayjs(sliced[0]?.date);
    filled = [];
    for (const x of sliced) {
      const thisDate = dayjs(x.date);
      while (currDate.isBefore(thisDate)) {
        filled.push({
          ...emptyDataPoint,
          date: currDate.format("YYYY-MM-DD"),
        });
        currDate = currDate.add(1, "day");
      }
      filled.push(x);
      currDate = thisDate.add(1, "day");
    }
    //categories.results.includes("Downloads") && console.log(filled);
  } else {
    filled = sliced;
  }

  return {
    data: filled,
    categories: categories.results,
    xAxis: "date",
  };
};

/**
 * Formats normalized data for a bar chart (for Tremor)
 * @param xAxis
 * @param data
 * @returns
 */
const formatDataToBarList = (xAxis: XAxis, data: EventData[]) => {
  const grouped = _.groupBy(data, (x) =>
    xAxis === "date"
      ? x.date
      : xAxis === "entity"
        ? x.entityName
        : xAxis === "metric"
          ? x.metricName
          : assertNever(xAxis),
  );
  const summed = _.mapValues(grouped, (x) => _.sumBy(x, (x) => x.amount));
  const result = _.toPairs(summed).map((x) => ({
    name: xAxis === "date" ? eventTimeToLabel(x[0]) : x[0],
    value: x[1],
  }));
  return {
    data: result,
  };
};

type CategoryOpts = {
  // Show the metric name
  includeMetric?: boolean;
  // Show the entity name (i.e. artifact, project, collection)
  includeEntity?: boolean;
};

/**
 * Creates unique categories for the area chart
 * @param entityName
 * @param metricName
 * @param opts
 * @returns
 */
const createCategory = (
  entityName: string,
  metricName: string,
  opts?: CategoryOpts,
) => {
  if (opts?.includeEntity && opts?.includeMetric) {
    return `${metricName}: ${entityName}`;
  } else if (opts?.includeEntity) {
    return `${entityName}`;
  } else {
    return `${metricName}`;
  }
};

/**
 * Create all categories
 */
const createCategories = (props: MetricsDataProviderProps) => {
  const entityIds = props.entityIds ?? [];
  const metricIds = props.metricIds ?? [];
  const results: string[] = [];
  const opts: CategoryOpts = {
    includeMetric: metricIds.length > 1,
    includeEntity: entityIds.length > 1,
  };
  for (const entityId of entityIds) {
    for (const metricId of metricIds) {
      results.push(createCategory(entityId, metricId, opts));
    }
  }
  return {
    results,
    opts,
  };
};

/**
 * Switches on the props to return formatted data
 * @param props - component props
 * @param rawData - normalized data from a GraphQL query
 * @returns
 */
const formatData = (
  props: MetricsDataProviderProps,
  rawData: EventData[],
  formatOpts?: FormatOpts,
) => {
  //const checkedData = rawData as unknown as EventData[];
  // Short-circuit if test data
  const data = props.useTestData
    ? uncheckedCast<EventData[]>(props.testData)
    : rawData;

  const formattedData =
    props.chartType === "areaChart"
      ? formatDataToAreaChart(data, createCategories(props), formatOpts)
      : props.chartType === "barList"
        ? formatDataToBarList(props.xAxis ?? DEFAULT_XAXIS, data)
        : assertNever(props.chartType);
  return formattedData;
};

/**
 * Generic MetricsDataProvider
 * @param props
 * @returns
 */
function MetricsDataProvider(props: MetricsDataProviderProps) {
  return props.entityType === "artifact" ? (
    <ArtifactMetricsDataProvider {...props} />
  ) : props.entityType === "project" ? (
    <ProjectMetricsDataProvider {...props} />
  ) : (
    assertNever(props.entityType)
  );
}

function ArtifactMetricsDataProvider(props: MetricsDataProviderProps) {
  useEnsureAuth();
  const bucketWidth = getBucketWidth(props);
  const {
    data: rawData,
    error: dataError,
    loading: dataLoading,
  } = useQuery(GET_TIMESERIES_METRICS_BY_ARTIFACT, {
    variables: {
      artifactIds: props.entityIds ?? [],
      metricIds: props.metricIds ?? [],
      startDate: eventTimeToLabel(props.startDate ?? DEFAULT_START_DATE),
      endDate: eventTimeToLabel(props.endDate),
    },
  });

  const metricIdToName: Record<string, string> = _.fromPairs(
    rawData?.oso_metricsV0?.map((x: any) => [
      ensure<string>(x.metricId, "Missing metricId"),
      ensure<string>(x.metricName, "Missing metricName"),
    ]),
  );
  const entityIdToName: Record<string, string> = _.fromPairs(
    rawData?.oso_artifactsV1?.map((x: any) => [
      ensure<string>(x.artifactId, "Missing artifactId"),
      ensure<string>(x.artifactName, "Missing artifactName"),
    ]),
  );
  const normalizedData: EventData[] = ensure(
    rawData?.oso_timeseriesMetricsByArtifactV0,
    "Data missing time series metrics",
  ).map((x: any) => ({
    metricId: ensure<string>(x.metricId, "Data missing 'metricId'"),
    metricName: ensure<string>(
      metricIdToName[x.metricId],
      "Data missing 'metricName'",
    ),
    entityId: ensure<string>(x.artifactId, "Data missing 'artifactId'"),
    entityName: ensure<string>(
      entityIdToName[x.artifactId],
      "Data missing 'artifactName'",
    ),
    date: ensure<string>(x.sampleDate, "Data missing 'sampleDate'"),
    amount: ensure<number>(x.amount, "Data missing 'amount'"),
  }));
  const formattedData = formatData(props, normalizedData, {
    gapFill: bucketWidth === "day",
  });
  !dataLoading && console.log(props, rawData, dataError, formattedData);
  return (
    <DataProviderView
      {...props}
      formattedData={formattedData}
      loading={dataLoading}
      error={dataError}
    />
  );
}

function ProjectMetricsDataProvider(props: MetricsDataProviderProps) {
  useEnsureAuth();
  const bucketWidth = getBucketWidth(props);
  const {
    data: rawData,
    error: dataError,
    loading: dataLoading,
  } = useQuery(GET_TIMESERIES_METRICS_BY_PROJECT, {
    variables: {
      projectIds: props.entityIds ?? [],
      metricIds: props.metricIds ?? [],
      startDate: eventTimeToLabel(props.startDate ?? DEFAULT_START_DATE),
      endDate: eventTimeToLabel(props.endDate),
    },
  });

  const metricIdToName: Record<string, string> = _.fromPairs(
    rawData?.oso_metricsV0?.map((x: any) => [
      ensure<string>(x.metricId, "Missing metricId"),
      ensure<string>(x.metricName, "Missing metricName"),
    ]),
  );
  const entityIdToName: Record<string, string> = _.fromPairs(
    rawData?.oso_projectsV1?.map((x: any) => [
      ensure<string>(x.projectId, "Missing projectId"),
      ensure<string>(x.projectName, "Missing projectName"),
    ]),
  );
  const normalizedData: EventData[] = ensure(
    rawData?.oso_timeseriesMetricsByProjectV0,
    "Data missing time series metrics",
  ).map((x: any) => ({
    metricId: ensure<string>(x.metricId, "Data missing 'metricId'"),
    metricName: ensure<string>(
      metricIdToName[x.metricId],
      "Data missing 'metricName'",
    ),
    entityId: ensure<string>(x.projectId, "Data missing 'projectId'"),
    entityName: ensure<string>(
      entityIdToName[x.projectId],
      "Data missing 'projectName'",
    ),
    date: ensure<string>(x.sampleDate, "Data missing 'sampleDate'"),
    amount: ensure<number>(x.amount, "Data missing 'amount'"),
  }));
  const formattedData = formatData(props, normalizedData, {
    gapFill: bucketWidth === "day",
  });
  !dataLoading && console.log(props, rawData, dataError, formattedData);
  return (
    <DataProviderView
      {...props}
      formattedData={formattedData}
      loading={dataLoading}
      error={dataError}
    />
  );
}

export { MetricsDataProviderRegistration, MetricsDataProvider };
export type { MetricsDataProviderProps };
