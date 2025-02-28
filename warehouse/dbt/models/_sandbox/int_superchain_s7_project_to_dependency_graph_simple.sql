{{
  config(
    materialized='table'
  )
}}


select distinct
  onchain_builder_project_id,
  devtooling_project_id,
  dependency_source
from {{ ref('int_superchain_s7_project_to_dependency_graph') }}
