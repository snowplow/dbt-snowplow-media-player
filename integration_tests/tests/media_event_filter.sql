{% set v1_names = ['playlist_deleted'] %}
{% set v2_names = ['play_event'] %}

with events as (
  select 'playlist_deleted' as event_name, 'com.example' as event_vendor, true as expected_default, true as expected_listed
  union all select 'Playlist_Deleted', 'com.example', true, true
  union all select 'play_event', 'COM.SNOWPLOWANALYTICS.SNOWPLOW.MEDIA', true, true
  union all select 'Play_Event', 'com.snowplowanalytics.snowplow.media', true, true
  union all select 'play_event', 'com.example', false, false
  union all select 'chapter_start_event', 'com.snowplowanalytics.snowplow.media', true, false
)
select *
from events
where ({{ snowplow_media_player.event_name_filter(v1_names) }}) <> expected_default
   or ({{ snowplow_utils.event_scan_filter(snowplow_media_player.media_event_filter(v1_names, v2_names)) }}) <> expected_listed
