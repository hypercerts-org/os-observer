{% macro ossd_filtered_blockchain_events(artifact_namespace, source_name, source_table) %}
with ossd_addresses as (
  select distinct `artifact_source_id` as `address`
  from {{ ref("int_ossd__artifacts_by_project") }} 
  where `artifact_namespace` = '{{ artifact_namespace }}'
)
select * 
from {{ oso_source(source_name, source_table)}}
where 
  to_address in (select * from ossd_addresses) 
  and from_address in (select * from ossd_addresses)
  {% if is_incremental() %}
    {# 
      We are using insert_overwrite so this will consistently select everything
      that would go into the latest partition (and any new partitions after
      that). It will overwrite any data in the partitions for which this select
      statement matches
    #}
    and block_timestamp > TIMESTAMP_SUB(_dbt_max_partition, INTERVAL 1 DAY)
  {% endif %}
{% endmacro %}