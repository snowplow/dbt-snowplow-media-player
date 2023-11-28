{% docs macro_dtype_to_type %}
{% raw %}
This macro retrieves the database specific data type from the dtype property.

#### Returns

dbt `{{ type_...() }}` macro.

{% endraw %}
{% enddocs %}

{% docs macro_field_alias %}
This macro returns a field alias in snake case with the prefix if set.

#### Returns

Field alias.

#### Usage

```sql
field_alias(field={'field': 'sessionId', 'dtype': 'string'}, prefix='media_session_')
```
Returns `media_session__session_id`.

{% raw %}
{% endraw %}
{% enddocs %}

{% docs macro_get_context_fields %}
{% raw %}
This macro returns specified fields from a context column.

#### Returns

Fields from context column. If `enabled` is false, it casts the fields as nulls.

#### Usage

```sql
{{ get_context_fields(
      enabled=var('snowplow__enable_whatwg_video', false),
      context='contexts_org_whatwg_video_element_1',
      prefix='html5_video_element_',
      fields=[
        {'field':'videoWidth', 'dtype':'integer'},
        {'field':'videoHeight', 'dtype':'integer'},
      ]) }}
```

{% endraw %}
{% enddocs %}

{% docs macro_get_enabled_context_fields %}
{% raw %}
This macro is used in the `get_context_fields` macro and returns fields from a context column if enabled. For BigQuery, it uses the `snowplow_utils.get_optional_fields`, else the `snowplow_utils.get_fields` macro is used. For Postgres/Redshift nothing is returned as context fields are already extracted in the base macro.

#### Returns

Fields from context column.

{% endraw %}
{% enddocs %}

{% docs macro_snakeify_case %}
{% raw %}
This macro takes a string in camel/pascal case and transforms it to snake case.

#### Returns

String in snake case.

#### Usage

```sql
{{ snakeify_case('mediaSessionId') }}
```
Returns `media_session_id`

{% endraw %}
{% enddocs %}


{% docs macro_allow_refresh %}
{% raw %}
This macro is used to determine if a full-refresh is allowed (depending on the environment), using the `snowplow__allow_refresh` variable.

#### Returns

`snowplow__allow_refresh` if environment is not `dev`, `none` otherwise.

{% endraw %}
{% enddocs %}

{% docs macro_event_name_filter %}
{% raw %}
This macro is used to add a filter on `event_name` if provided.

#### Returns

Filter for `event_name` values inputted `event_names` list or `lower(event_vendor) = 'com.snowplowanalytics.snowplow.media'`
{% endraw %}
{% enddocs %}

{% docs macro_get_percentage_boundaries %}
{% raw %}
This macro gets the list of percentage boundaries which are set in the tracker.

#### Returns

Percentage boundaries, e.g. `[25,50,75,100]`
{% endraw %}
{% enddocs %}

{% docs macro_session_identifiers %}
{% raw %}
This macro is used to set the `session_identifier` used in the base macros.

#### Returns

`snowplow__session_identifiers` if set. Otherwise it defaults to the media session id if enabled, else the page/screen view id from web and mobile.
{% endraw %}
{% enddocs %}

{% docs macro_user_identifiers %}
{% raw %}
This macro is used to set the `user_identifier` used in the base macros.

#### Returns

`snowplow__user_identifiers` if set. Otherwise it defaults to the domain_userid for web or user_id from the client session context for mobile.
{% endraw %}
{% enddocs %}

{% docs macro_media_event_type_field %}
{% raw %}
Retrieves the event type either from the media player event in case of v1 media schemas or the event name in case of v2 media schemas.

#### Returns

The query for the event_type column.

{% endraw %}
{% enddocs %}

{% docs macro_media_player_type_field %}
{% raw %}
This macro produces the value media_player_type column in the snowplow_media_player_base_events_this_run table based on the values of the youtube_player_id and media_player_id columns.

#### Returns

The query for the media_player_type column.

{% endraw %}
{% enddocs %}

{% docs macro_media_type_field %}
{% raw %}
This macro produces the value media_type column in the snowplow_media_player_base_events_this_run table based on the column for media_type in the media context or returns video for youtube.

#### Returns

The query for the media_type column.

{% endraw %}
{% enddocs %}

{% docs macro_percent_progress_field %}
{% raw %}
This macro produces the value for the percentage progress in case of percent progress events.
For v1 media schemas, the value is taken from the media player context entity.
For v2 media schemas, it is calculated based on the current time, duration and defined percentage boundaries.

#### Returns

The query for the percent_progress field.

{% endraw %}
{% enddocs %}

{% docs macro_playback_quality_field %}
{% raw %}
This macro produces the value for the playback_quality column in the snowplow_media_player_base_events_this_run table based on the values of the quality in youtube context or video_width and video_height columns in media context.

#### Returns

The query for the playback_quality column.

{% endraw %}
{% enddocs %}

{% docs macro_config_check %}
{% raw %}

A macro that checks if at least one of the platform enabling variables is true and if the media player contexts variable configuration is valid before the run starts. Raises and error to alert users in the case the variable configuration is not valid.

{% endraw %}
{% enddocs %}
