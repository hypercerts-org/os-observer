from datetime import datetime, timedelta
from typing import Mapping

from dagster import (
    DefaultSensorStatus,
    MultiAssetSensorEvaluationContext,
    OpExecutionContext,
    RunConfig,
    RunFailureSensorContext,
    RunRequest,
    SkipReason,
    job,
    multi_asset_sensor,
    op,
    run_failure_sensor,
)

from ..utils import (
    AlertManager,
    AlertOpConfig,
    FreshnessOpConfig,
    stable_source_tag,
    unstable_source_tag,
)
from .common import AssetFactoryResponse

ALERTS_JOB_CONFIG = {
    "dagster-k8s/config": {
        "merge_behavior": "SHALLOW",
        "container_config": {
            "resources": {
                "requests": {
                    "cpu": "500m",
                    "memory": "1024Mi",
                },
                "limits": {
                    "memory": "2048Mi",
                },
            },
        },
        "pod_spec_config": {
            "node_selector": {
                "pool_type": "persistent",
            },
            "tolerations": [
                {
                    "key": "pool_type",
                    "operator": "Equal",
                    "value": "persistent",
                    "effect": "NoSchedule",
                }
            ],
        },
    },
}

def setup_alert_sensors(
    base_url: str, alert_manager: AlertManager, enable: bool = True
):
    @op(name="failure_alert_op")
    def failure_op(context: OpExecutionContext, config: AlertOpConfig) -> None:
        alert_manager.failure_op(base_url, context, config)

    @job(name="failure_alert_job")
    def failure_job():
        failure_op()

    @op(name="freshness_alert_op")
    def freshness_alert_op(
        context: OpExecutionContext, config: FreshnessOpConfig
    ) -> None:
        alert_manager.freshness_op(base_url, config)

    @job(name="freshness_alert_job")
    def freshness_alert_job():
        freshness_alert_op()

    if enable:
        status = DefaultSensorStatus.RUNNING
    else:
        status = DefaultSensorStatus.STOPPED

    @run_failure_sensor(
        name="failure_alert", default_status=status, request_job=failure_job
    )
    def failure_sensor(context: RunFailureSensorContext):
        if context.failure_event.job_name not in [
            "materialize_stable_source_assets_job",
            "materialize_core_assets_job",
        ]:
            return SkipReason("Non critical job failure")

        return RunRequest(
            tags=ALERTS_JOB_CONFIG,
            run_key=context.dagster_run.run_id,
            run_config=RunConfig(
                ops={
                    "failure_alert_op": {
                        "config": {
                            "run_id": context.dagster_run.run_id,
                        }
                    }
                }
            ),
        )

    # Only validates assets that have materialized at least once successfully
    @multi_asset_sensor(
        monitored_assets=stable_source_tag | unstable_source_tag,
        job=freshness_alert_job,
        default_status=status,
        minimum_interval_seconds=259200,  # 3 days
    )
    def freshness_check_sensor(context: MultiAssetSensorEvaluationContext):
        materialization_records = context.latest_materialization_records_by_key(
            context.asset_keys
        )

        stale_assets: Mapping[str, float] = {}

        context.log.info(
            f"Checking freshness of {len(materialization_records.items())} assets"
        )

        for asset_key, record in materialization_records.items():
            if record is None:
                continue

            if (
                datetime.now().timestamp() - record.event_log_entry.timestamp
                > timedelta(weeks=1).total_seconds()
            ):
                stale_assets[asset_key.to_user_string()] = (
                    record.event_log_entry.timestamp
                )

        context.log.info(f"Found {len(stale_assets)} stale assets")

        # Reset the cursor to always check the latest materialization
        for asset_key in context.asset_keys:
            context.advance_cursor({asset_key: None})

        return RunRequest(
            tags=ALERTS_JOB_CONFIG,
            run_config=RunConfig(
                ops={
                    "freshness_alert_op": {
                        "config": {
                            "fresh_assets": len(materialization_records)
                            - len(stale_assets),
                            "stale_assets": stale_assets,
                        }
                    }
                }
            ),
        )

    return AssetFactoryResponse(
        [],
        sensors=[failure_sensor, freshness_check_sensor],
        jobs=[failure_job, freshness_alert_job],
    )
