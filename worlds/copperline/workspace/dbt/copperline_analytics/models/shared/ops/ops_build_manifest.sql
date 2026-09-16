{{
    config(
        materialized='table'
    )
}}

{#-
    What the graph looked like on the run that built this table.

    One row per model, with its layer, its owning team, its materialisation and
    the schema it lands in. It is read off `graph` at compile time, which means
    it describes the project as parsed — not as run, and not as committed.

    Two things use it. `plat_manifest_check` compares it to the committed
    manifest and warns when they differ, which is how a model added without
    regenerating the manifest gets noticed. `plat_lineage_publish` reads the
    same graph for `ops/lineage.json`.

    **It holds `ref` edges only.** docs/lineage.md is the list of every consumer
    of every mart, including the dashboards, the exports, the config-assembled
    readers and the two name-pattern sensors, and this table cannot see any of
    them. Six of the twelve consumers on `marts.order_economics` are invisible
    here. `dbt ls` is not the list, and neither is this.
-#}

{% set models = [] %}
{% if execute %}
    {% for node in graph.nodes.values() if node.resource_type == 'model' %}
        {% set counts = namespace(models=0, sources=0) %}
        {% for dep in node.depends_on.nodes %}
            {% if dep.startswith('model.') %}
                {% set counts.models = counts.models + 1 %}
            {% elif dep.startswith('source.') %}
                {% set counts.sources = counts.sources + 1 %}
            {% endif %}
        {% endfor %}
        {% do models.append({
            'name': node.name,
            'path': node.path,
            'materialized': node.config.materialized,
            'schema': node.schema,
            'tags': node.config.tags | join(','),
            'deps': node.depends_on.nodes | length,
            'model_deps': counts.models,
            'source_deps': counts.sources
        }) %}
    {% endfor %}
{% endif %}

with parsed as (

select * from (
    values
    {% for n in models %}
        (
            '{{ n.name }}',
            '{{ n.path | replace("\\\\", "/") }}',
            '{{ n.materialized }}',
            '{{ n.schema }}',
            '{{ n.tags }}',
            {{ n.deps }},
            {{ n.model_deps }},
            {{ n.source_deps }}
        ){{ "," if not loop.last }}
    {% endfor %}
) as t (
    model_name,
    model_path,
    materialization,
    target_schema,
    tags,
    dependency_count,
    model_dependency_count,
    source_dependency_count
)

)

select
    model_name,
    model_path,
    materialization,
    target_schema,
    tags,

    -- Layer and owner come from where the file sits. That is the convention and
    -- it is the only thing that enforces it: a mart in models/shared/ or a
    -- staging model inside a team directory shows up here as what its path says
    -- it is, which is how the departure gets found.
    case
        when model_path like 'shared/staging/%'       then 'stg'
        when model_path like '%intermediate/%'        then 'int'
        when model_path like 'shared/dims/%'          then 'dim'
        when model_path like 'shared/ops/%'           then 'ops'
        else 'mart'
    end                                             as layer,

    case
        when model_path like 'shared/%'     then 'platform'
        else split_part(model_path, '/', 1)
    end                                             as owning_team,

    dependency_count,
    model_dependency_count,
    source_dependency_count,
    {{ ds() }}                                      as built_for_ds

from parsed
