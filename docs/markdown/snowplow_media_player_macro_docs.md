{% docs macro_media_id_col %}
{% raw %}
This macro produces the value media_id column in the snowplow_media_player_base_events_this_run table based on the values of the youtube_player_id and media_player_id columns.

#### Returns

The query for the media_id column.

#### Usage

```sql
select
    ...,
    {{ media_id_col(
        youtube_player_id='a.contexts_com_youtube_youtube_1[0]:playerId',
        media_player_id='a.contexts_org_whatwg_media_element_1[0]:htmlId::varchar'
    ) }}
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_player_type_col %}
{% raw %}
This macro produces the value media_player_type column in the snowplow_media_player_base_events_this_run table based on the values of the youtube_player_id and media_player_id columns.

#### Returns

The query for the media_player_type column.

#### Usage

```sql
select
    ...,
    {{ media_player_type_col(
        youtube_player_id='a.contexts_com_youtube_youtube_1[0]:playerId',
        media_player_id='a.contexts_org_whatwg_media_element_1[0]:htmlId::varchar'
    ) }}
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_media_type_col %}
{% raw %}
This macro produces the value media_type column in the snowplow_media_player_base_events_this_run table based on the column for media_type in the media context or returns video for youtube.

#### Returns

The query for the media_type column.

#### Usage

```sql
select
    ...,
    {{ media_type_col(
      media_media_type='a.contexts_org_whatwg_media_element_1[0]:mediaType::varchar'
    ) }}
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_playback_quality_col %}
{% raw %}
This macro produces the value for the playback_quality column in the snowplow_media_player_base_events_this_run table based on the values of the quality in youtube context or video_width and video_height columns in media context.

#### Returns

The query for the playback_quality column.

#### Usage

```sql
select
    ...,
    {{ playback_quality_col(
      youtube_quality='a.contexts_com_youtube_youtube_1[0]:playbackQuality::varchar',
      video_width='a.contexts_org_whatwg_video_element_1[0]:videoWidth::varchar',
      video_height='a.contexts_org_whatwg_video_element_1[0]:videoHeight::varchar'
    )}}
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}

{% docs macro_source_url_col %}
{% raw %}
This macro produces the value source_url column in the snowplow_media_player_base_events_this_run table based on the columns for the url in YouTube context and current_src in media context.

#### Returns

The query for the source_url column.

#### Usage

```sql
select
    ...,
    {{ source_url_col(
      youtube_url='a.contexts_com_youtube_youtube_1[0]:url::varchar',
      media_current_src='a.contexts_org_whatwg_media_element_1[0]:currentSrc::varchar'
    ) }}
    from {{ var('snowplow__events') }} as a
```

{% endraw %}
{% enddocs %}
