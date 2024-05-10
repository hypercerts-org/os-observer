import re
from enum import Enum

import arrow
from google.api_core.exceptions import NotFound
from google.cloud.bigquery.job import CopyJobConfig
import pandas as pd
from dataclasses import dataclass
from dagster import (
    asset,
    asset_sensor,
    job,
    op,
    SensorEvaluationContext,
    AssetExecutionContext,
    MaterializeResult,
    EventLogEntry,
    RunRequest,
    RunConfig,
    OpExecutionContext,
    DefaultSensorStatus,
)
from dagster_gcp import BigQueryResource, GCSResource

from .common import AssetFactoryResponse
from ..utils.bq import ensure_dataset, DatasetOptions


class Interval(Enum):
    Hourly = 0
    Daily = 1
    Weekly = 2
    Monthly = 3


class SourceMode(Enum):
    Incremental = 0
    Overwrite = 1


@dataclass(kw_only=True)
class BaseGCSAsset:
    project_id: str
    bucket_name: str
    path_base: str
    file_match: str
    destination_table: str
    raw_dataset_name: str
    clean_dataset_name: str
    format: str = "CSV"


@dataclass(kw_only=True)
class IntervalGCSAsset(BaseGCSAsset):
    interval: Interval
    mode: SourceMode
    retention_days: int


def parse_interval_prefix(interval: Interval, prefix: str) -> arrow.Arrow:
    return arrow.get(prefix, "YYYYMMDD")


def interval_gcs_import_asset(key: str, config: IntervalGCSAsset, **kwargs):
    # Find all of the "intervals" in the bucket and load them into the `raw_sources` dataset
    # Run these sources through a secondary dbt model into `clean_sources`

    @asset(key=key, **kwargs)
    def gcs_asset(
        context: AssetExecutionContext, bigquery: BigQueryResource, gcs: GCSResource
    ) -> MaterializeResult:
        # Check the current state of the bigquery db (we will only load things
        # that are new than that). We continously store the imported data in
        # {project}.{dataset}.{table}_{interval_prefix}.
        with bigquery.get_client() as bq_client:
            ensure_dataset(
                bq_client,
                DatasetOptions(
                    dataset_ref=bq_client.dataset(dataset_id=config.clean_dataset_name),
                    is_public=True,
                ),
            )

            clean_dataset = bq_client.get_dataset(config.clean_dataset_name)
            clean_table_ref = clean_dataset.table(config.destination_table)

            current_source_date = arrow.get("1970-01-01")
            try:
                clean_table = bq_client.get_table(clean_table_ref)
                current_source_date = arrow.get(
                    clean_table.labels.get("source_date", "1970-01-01")
                )
            except NotFound as exc:
                if config.destination_table in exc.message:
                    context.log.info("Cleaned destination table not found.")
                else:
                    raise exc

            client = gcs.get_client()
            blobs = client.list_blobs(config.bucket_name, prefix=config.path_base)

            file_matcher = re.compile(config.path_base + "/" + config.file_match)

            matching_blobs = []

            # List all of the files in the prefix
            for blob in blobs:
                match = file_matcher.match(blob.name)
                if not match:
                    context.log.debug(f"skipping {blob.name}")
                    continue
                try:
                    interval_timestamp = arrow.get(
                        match.group("interval_timestamp"), "YYYY-MM-DD"
                    )
                    matching_blobs.append((interval_timestamp, blob.name))
                except IndexError:
                    context.log.debug(f"skipping {blob.name}")
                    continue

            sorted_blobs = sorted(
                matching_blobs, key=lambda a: a[0].int_timestamp, reverse=True
            )

            if len(sorted_blobs) == 0:
                context.log.info("no existing data found")
                return MaterializeResult(
                    metadata={
                        "updated": False,
                        "files_loaded": 0,
                        "latest_source_date": current_source_date.format("YYYY-MM-DD"),
                    }
                )

            latest_source_date = sorted_blobs[0][0]

            if latest_source_date.int_timestamp <= current_source_date.int_timestamp:
                context.log.info("no updated data found")

                return MaterializeResult(
                    metadata={
                        "updated": False,
                        "files_loaded": 0,
                        "latest": current_source_date.format("YYYY-MM-DD"),
                    }
                )

            blob_name = sorted_blobs[0][1]

            interval_table = f"{config.project_id}.{config.raw_dataset_name}.{config.destination_table}__{latest_source_date.format('YYYYMMDD')}"

            # Run the import of the latest data and overwrite the data
            bq_client.query_and_wait(
                f"""
            LOAD DATA OVERWRITE `{interval_table}`
            FROM FILES (
                format = "{config.format}",
                uris = ["gs://{config.bucket_name}/{blob_name}"]
            );
            """
            )

            raw_table_ref = bq_client.get_dataset(config.raw_dataset_name).table(
                f"{config.destination_table}__{latest_source_date.format('YYYYMMDD')}"
            )

            copy_job_config = CopyJobConfig(write_disposition="WRITE_TRUNCATE")

            # The clean table is just the overwritten data without any date.
            # We keep old datasets around in case we need to rollback for any reason.
            job = bq_client.copy_table(
                raw_table_ref,
                clean_table_ref,
                location="US",
                job_config=copy_job_config,
            )
            job.result()

            latest_source_date_str = latest_source_date.format("YYYY-MM-DD")

            clean_table = bq_client.get_table(clean_table_ref)
            labels = clean_table.labels
            labels["source_date"] = latest_source_date_str
            clean_table.labels = labels
            bq_client.update_table(clean_table, fields=["labels"])

            return MaterializeResult(
                metadata={
                    "updated": True,
                    "files_loaded": 1,
                    "latest_source_date": latest_source_date_str,
                }
            )

    @op(name=f"{key}_clean_up_op")
    def gcs_clean_up_op(context: OpExecutionContext, config: dict):
        context.log.info(f"Running clean up for {key}")
        print(config)

    @job(name=f"{key}_clean_up_job")
    def gcs_clean_up_job():
        gcs_clean_up_op()

    @asset_sensor(
        asset_key=gcs_asset.key,
        name=f"{key}_clean_up_sensor",
        job=gcs_clean_up_job,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def gcs_clean_up_sensor(
        context: SensorEvaluationContext, gcs: GCSResource, asset_event: EventLogEntry
    ):
        print("EVENT!!!!!!")
        yield RunRequest(
            run_key=context.cursor,
            run_config=RunConfig(
                ops={f"{key}_clean_up_op": {"config": {"asset_event": asset_event}}}
            ),
        )

    return AssetFactoryResponse([gcs_asset], [gcs_clean_up_sensor], [gcs_clean_up_job])
