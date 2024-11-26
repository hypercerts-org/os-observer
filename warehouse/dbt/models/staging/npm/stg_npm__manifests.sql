{% set columns = [
    "name", "version", "description", "keywords", "homepage", "bugs", 
    "license", "author", "contributors", "funding", "files", "exports", 
    "main", "browser", "bin", "man", "directories", "repository", 
    "scripts", "config", "dependencies", "dev_dependencies", 
    "peer_dependencies", "peer_dependencies_meta", "bundle_dependencies", 
    "optional_dependencies", "overrides", "engines", "os", "cpu", 
    "dev_engines", "private", "publish_config", "workspaces", "bugs__url", 
    "repository__url", "repository__type",  "author__url", "author__name",
    "author__email"
] %}

with source as (
  select * from {{ source('npm', 'manifests') }}
),

renamed as (
  select
    {% for column in columns %}
      {{ adapter.quote(column) }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from source
)

select * from renamed
