{% docs macro_field %}
{% raw %}
This macro is used to define a path to a column either as a string or using a dictionary definition.

On BigQuery, the `snowplow_utils.get_optional_fields` macro is used.

#### Returns

The query path for the field.

#### Usage

```sql
select
    ...,
    {{ field('a.contexts_com_youtube_youtube_1[0]:playerId') }},
    {{ field({ 'field': 'playerId', 'col_prefix': 'a.contexts_com_youtube_youtube_1' }) }},
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_ad_break_field %}
{% raw %}
This macro retrieves a property from the media ad break context entity.

#### Returns

The query path for the field.

#### Usage

```sql
select
    ...,
    {{ media_ad_break_field({ 'field': 'name' }) }},
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_ad_field %}
{% raw %}
This macro retrieves a property from the media ad context entity.

#### Returns

The query path for the field.

#### Usage

```sql
select
    ...,
    {{ media_ad_field({ 'field': 'name' }) }},
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_ad_quartile_event_field %}
{% raw %}
This macro retrieves a property from the media ad quartile self-describing event.

#### Returns

The query path for the field.

#### Usage

```sql
select
    ...,
    {{ media_ad_quartile_event_field({ 'field': 'percent_progress' }) }},
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_event_type_field %}
{% raw %}
Retrieves the event type either from the media player event in case of v1 media schemas or the event name in case of v2 media schemas.

#### Returns

The query path for the field.

#### Usage

```sql
select
    ...,
    {{ media_event_type_field(media_player_event_type={ 'dtype': 'string' }, event_name='a.event_name') }} as event_type,
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_player_field %}
{% raw %}
This macro retrieves a property from either the v2 or v1 media player context entity.

#### Returns

The query path for the field.

#### Usage

```sql
select
    ...,
    round({{ media_player_field(
      v1={ 'field': 'duration', 'dtype': 'double' },
      v2={ 'field': 'duration', 'dtype': 'double' }
    ) }}) as duration_secs,
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_player_id_field %}
{% raw %}
This macro produces the value player_id column in the snowplow_media_player_base_events_this_run table based on the values of the youtube_player_id and media_player_id columns.

#### Returns

The query for the player_id column.

#### Usage

```sql
select
    ...,
    {{ player_id_field(
        youtube_player_id='a.contexts_com_youtube_youtube_1[0]:playerId',
        media_player_id='a.contexts_org_whatwg_media_element_1[0]:htmlId::varchar'
    ) }} as player_id
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_player_type_field %}
{% raw %}
This macro produces the value media_player_type column in the snowplow_media_player_base_events_this_run table based on the values of the youtube_player_id and media_player_id columns.

#### Returns

The query for the media_player_type column.

#### Usage

```sql
select
    ...,
    {{ media_player_type_field(
        youtube_player_id='a.contexts_com_youtube_youtube_1[0]:playerId',
        media_player_id='a.contexts_org_whatwg_media_element_1[0]:htmlId::varchar'
    ) }} as media_player_type
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_session_field %}
{% raw %}
This macro retrieves a property from the media session context entity.

#### Returns

The query path for the field.

#### Usage

```sql
select
    ...,
    {{ media_session_field({ 'field': 'time_played' }) }},
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_type_field %}
{% raw %}
This macro produces the value media_type column in the snowplow_media_player_base_events_this_run table based on the column for media_type in the media context or returns video for youtube.

#### Returns

The query for the media_type column.

#### Usage

```sql
select
    ...,
    {{ media_type_field(
      media_media_type='a.contexts_org_whatwg_media_element_1[0]:mediaType::varchar'
    ) }} as media_type
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_percent_progress_field %}
{% raw %}
This macro produces the value for the percentage progress in case of percent progress events.
For v1 media schemas, the value is taken from the media player context entity.
For v2 media schemas, it is calculated based on the current time, duration and defined percentage boundaries.

#### Returns

The query for the percent_progress field.

#### Usage

```sql
select
    ...,
    {{ percent_progress_field(
        v1_percent_progress={ 'field': 'percent_progress', 'dtype': 'string' },
        v1_event_type={ 'field': 'type', 'dtype': 'string' },
        event_name='a.event_name',
        v2_current_time={ 'field': 'current_time', 'dtype': 'double' },
        v2_duration={ 'field': 'duration', 'dtype': 'double' }
    ) }} as percent_progress
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_playback_quality_field %}
{% raw %}
This macro produces the value for the playback_quality column in the snowplow_media_player_base_events_this_run table based on the values of the quality in youtube context or video_width and video_height columns in media context.

#### Returns

The query for the playback_quality column.

#### Usage

```sql
select
    ...,
    {{ playback_quality_field(
      youtube_quality='a.contexts_com_youtube_youtube_1[0]:playbackQuality::varchar',
      video_width='a.contexts_org_whatwg_video_element_1[0]:videoWidth::varchar',
      video_height='a.contexts_org_whatwg_video_element_1[0]:videoHeight::varchar'
    )}} as playback_quality
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_source_url_field %}
{% raw %}
This macro produces the value source_url column in the snowplow_media_player_base_events_this_run table based on the columns for the url in YouTube context and current_src in media context.

#### Returns

The query for the source_url column.

#### Usage

```sql
select
    ...,
    {{ source_url_field(
      youtube_url='a.contexts_com_youtube_youtube_1[0]:url::varchar',
      media_current_src='a.contexts_org_whatwg_media_element_1[0]:currentSrc::varchar'
    ) }} as source_url
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_web_or_mobile_field %}
{% raw %}
This macro retrieves a property from the given fields based on whether web or mobile or both events are enabled.

#### Returns

The query path for the field.

#### Usage

```sql
select
    ...,
    {{ web_or_mobile_field(
      web='a.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[safe_offset(0)].id',
      mobile={'field': 'id', 'col_prefix': 'contexts_com_snowplowanalytics_mobile_screen_1_' }
    ) }} as page_view_id,
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}
