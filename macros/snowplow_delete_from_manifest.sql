{% macro snowplow_media_player_delete_from_manifest(models) %}
    {{ snowplow_utils.snowplow_delete_from_manifest(models, ref('snowplow_media_player_incremental_manifest'))}}
{% endmacro %}
