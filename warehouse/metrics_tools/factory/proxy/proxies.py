import typing as t
from datetime import datetime

import pandas as pd
import sqlglot as sql
from metrics_tools.definition import PeerMetricDependencyRef
from metrics_tools.factory.generated import generated_rolling_query
from metrics_tools.factory.utils import metric_ref_evaluator_context
from sqlglot import exp
from sqlmesh import ExecutionContext
from sqlmesh.core.dialect import parse_one
from sqlmesh.core.macros import MacroEvaluator


def generated_query(
    evaluator: MacroEvaluator,
    *,
    rendered_query_str: str,
    ref: PeerMetricDependencyRef,
    table_name: str,
    vars: t.Dict[str, t.Any],
):
    """Simple generated query executor for metrics queries"""

    with metric_ref_evaluator_context(evaluator, ref, vars):
        result = evaluator.transform(parse_one(rendered_query_str))
    return result


def generated_rolling_query_proxy(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    # *,
    # ref: PeerMetricDependencyRef,
    # vars: t.Dict[str, t.Any],
    # rendered_query_str: str,
    # table_name: str,
    # sqlmesh_vars: t.Dict[str, t.Any],
    **kwargs,
) -> t.Iterator[pd.DataFrame | exp.Expression]:
    """This acts as the proxy to the actual function that we'd call for
    the metrics model."""
    ref = t.cast(PeerMetricDependencyRef, context.var("ref"))
    vars = t.cast(t.Dict[str, t.Any], context.var("vars"))
    rendered_query_str = t.cast(str, context.var("rendered_query_str"))
    table_name = t.cast(str, context.var("table_name"))

    yield from generated_rolling_query(
        context,
        start,
        end,
        execution_time,
        ref,
        vars,
        rendered_query_str,
        table_name,
        context.gateway,
        # Change the following variable to force reevaluation. Hack for now.
        "version=v5",
    )


def join_all_of_entity_type(
    evaluator: MacroEvaluator, *, db: str, tables: t.List[str], columns: t.List[str]
):
    # A bit of a hack but we know we have a "metric" column. We want to
    # transform this metric id to also include the event_source as a prefix to
    # that metric id in the joined table
    transformed_columns = []
    for column in columns:
        if column == "event_source":
            continue
        if column == "metric":
            transformed_columns.append(
                exp.alias_(
                    exp.Concat(
                        expressions=[
                            exp.to_column("event_source"),
                            exp.Literal(this="_", is_string=True),
                            exp.to_column(column),
                        ],
                        safe=False,
                        coalesce=False,
                    ),
                    alias="metric",
                )
            )
        else:
            transformed_columns.append(column)

    query = exp.select(*transformed_columns).from_(sql.to_table(f"{db}.{tables[0]}"))
    for table in tables[1:]:
        query = query.union(
            exp.select(*transformed_columns).from_(sql.to_table(f"{db}.{table}")),
            distinct=False,
        )
    # Calculate the correct metric_id for all of the entity types
    return query
