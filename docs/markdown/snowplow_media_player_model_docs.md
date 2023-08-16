{% docs table_interactions_this_run %}
This staging table shows all media player events within the current incremental run and calculates play_time. It could be used in custom models for more in-depth time based calculations.
{% enddocs %}

{% docs table_base_this_run %}
This staging table aggregates media player interactions within the current run to a pageview level that is considered a base level for media plays.
{% enddocs %}

{% docs table_base %}
This derived table aggregates media player interactions to a pageview level incrementally.
{% enddocs %}

{% docs table_plays_by_pageview %}
This view removes impressions from the derived snowplow_media_player_base table for showing pageview level media play events. This means that only media player instances where a play event was tracked are kept (where `is_played` is true).
{% enddocs %}

{% docs table_session_stats %}
This table aggregates the pageview level interactions to show session level media stats.
{% enddocs %}

{% docs table_user_stats %}
This table aggregates the pageview level interactions to show user level media stats.
{% enddocs %}

{% docs table_media_stats %}
This derived table aggregates the pageview level interactions to show overall media stats.
{% enddocs %}

{% docs table_pivot_base %}
This helper table serves as a base to calculate percent_progress based fields as well as the play_time metrics (by calculating the weight attributed to a percent progress being reached).
{% enddocs %}

{% docs table_media_ad_views_this_run %}

This derived table aggregates information about ad views. Each ad view (a user viewing a single ad within a media playback) is represented using one row. The ad views aggregate information about the percentage progress of the ad viewed, whether it was skipped, clicked or completed. They also contain meta information about the played ads.

{% enddocs %}

{% docs table_media_ads %}
This derived table aggregates information about ads. Each row represents one ad played within a certain media on a certain platform. Stats about the number of ad clicks, progress reached and more are calculated as total values but also as counts of unique users (identified using `domain_userid`).
{% enddocs %}
