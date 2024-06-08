{{
  config(
    materialized='table'
  )
}}

{% set networks = ["optimism", "base", "frax", "metal", "mode", "zora"] %}

{% set union_queries = [] %}

{% for network in networks %}
  {% set table_name = "stg_" ~ network ~ "__proxies" %}
  {% set network_upper = network.upper() %}

  {% set query %}
  select
    lower(to_address) as `address`,
    '{{ network_upper }}' as network,
    min(block_timestamp) as created_date
  from {{ ref(table_name) }}
  where
    proxy_type = "SAFE"
    and proxy_address != to_address
  group by to_address
  {% endset %}

  {% do union_queries.append(query) %}
{% endfor %}

{% set final_query = union_queries | join(' union all ') %}

with safes as (
  {{ final_query }}
)

select
  {{ oso_id("network", "address") }} as artifact_id,
  address,
  network,
  created_date
from safes
