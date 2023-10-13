import React from "react";
import { DataProviderView } from "./provider-view";
import type { CommonDataProviderProps } from "./provider-view";

/**
 * Query component focused on providing data to our data tables
 *
 * Current limitations:
 * - Does not support authentication or RLS. Make sure data is readable by unauthenticated users
 */
type TableDataProviderProps = CommonDataProviderProps & {
  ids?: string[];
  eventTypes?: string[];
  startDate?: string;
  endDate?: string;
};

/**
 * EventDataProvider for artifacts
 * @param props
 * @returns
 */
function TableDataProvider(props: TableDataProviderProps) {
  /**
  const variables = createVariables(props);
  const {
    data: rawData,
    error,
    loading,
  } = useQuery(GET_EVENTS_DAILY_BY_ARTIFACT, { variables });
  const normalizedData: EventData[] = (
    rawData?.events_daily_by_artifact ?? []
  ).map((x) => ({
    type: ensure<string>(x.type, "Data missing 'type'"),
    id: ensure<number>(x.toId, "Data missing 'projectId'"),
    date: ensure<string>(x.bucketDaily, "Data missing 'bucketDaily'"),
    amount: ensure<number>(x.amount, "Data missing 'number'"),
  }));
  console.log(props.ids, rawData, formattedData);
  */
  const loading = false;
  const formattedData = {};
  const error = undefined;
  return (
    <DataProviderView
      {...props}
      formattedData={formattedData}
      loading={loading}
      error={error}
    />
  );
}

export { TableDataProvider };
