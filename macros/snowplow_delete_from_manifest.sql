{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro snowplow_media_player_delete_from_manifest(models) %}
    {{ snowplow_utils.snowplow_delete_from_manifest(models, ref('snowplow_media_player_incremental_manifest'))}}
{% endmacro %}
