#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "databricks" "postgres" "redshift" "snowflake", "spark_iceberg")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "Snowplow media player integration tests: Seeding data"

  eval "dbt seed --target $db --full-refresh" || exit 1;

  # This run and the subsequent incremental ones exist just to make sure that the models work with the newer contexts disabled
  echo "Snowplow media player integration tests (v1 only): Execute models - run 1/2"

  eval "dbt run --target $db --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__enable_media_player_v2: false, snowplow__enable_media_session: false, snowplow__enable_media_ad: false, snowplow__enable_media_ad_break: false, snowplow__enable_ad_quartile_event: false, snowplow__enable_mobile_events: false}'" || exit 1;

  echo "Snowplow media player integration tests (v1 only): Execute models - run 2/2"

  eval "dbt run --target $db --vars '{snowplow__allow_refresh: true, snowplow__enable_media_player_v2: false, snowplow__enable_media_session: false, snowplow__enable_media_ad: false, snowplow__enable_media_ad_break: false, snowplow__enable_ad_quartile_event: false, snowplow__enable_mobile_events: false}'" || exit 1;

  # This run and the subsequent incremental ones exist just to make sure that the models work with the older contexts disabled
  echo "Snowplow media player integration tests (v2 only): Execute models - run 1/2"

  eval "dbt run --target $db --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 3000, snowplow__enable_youtube: false, snowplow__enable_whatwg_media: false, snowplow__enable_whatwg_video: false, snowplow__enable_media_player_v1: false}'" || exit 1;

  echo "Snowplow media player integration tests (v2 only): Execute models - run 2/2"

  eval "dbt run --target $db --vars '{snowplow__enable_youtube: false, snowplow__enable_whatwg_media: false, snowplow__enable_whatwg_video: false, snowplow__enable_media_player_v1: false}'" || exit 1;

  # The second pass sets the v2 name list, keeping the mixed-case v1 config, and must give the same results.
  for v2_vars in "snowplow__media_v2_event_names: null" "snowplow__media_v2_event_names: [ad_break_end_event, ad_break_start_event, ad_click_event, ad_complete_event, ad_pause_event, ad_quartile_event, ad_resume_event, ad_skip_event, ad_start_event, buffer_end_event, buffer_start_event, end_event, error_event, fullscreen_change_event, pause_event, percent_progress_event, picture_in_picture_change_event, ping_event, play_event, playback_rate_change_event, quality_change_event, ready_event, seek_end_event, seek_start_event, volume_change_event]"; do
    echo "Snowplow media player integration tests: Execute models - run 1/6"

    eval "dbt run --target $db --full-refresh --vars '{snowplow__allow_refresh: true, $v2_vars}'" || exit 1;

    for i in {2..3}
    do
      echo "Snowplow media player integration tests: Execute models - run $i/6"

      eval "dbt run --target $db --vars '{$v2_vars}'" || exit 1;
    done

    for i in {4..6}
    do
      echo "Snowplow media player integration tests: Execute models - run $i/6"

      eval "dbt run --target $db --vars '{snowplow__backfill_limit_days: 300, $v2_vars}'" || exit 1;
    done

    echo "Snowplow media player integration tests: Test models"

    eval "dbt test --target $db --vars '{$v2_vars}'" || exit 1;
  done

  echo "Snowplow media player integration tests: Testing ad views passthrough - mixed configuration"
  eval "dbt run --select +snowplow_media_player_media_ad_views_this_run --target $db --full-refresh --vars '{
    snowplow__allow_refresh: true,
    snowplow__enable_media_ad: true,
    snowplow__backfill_limit_days: 999,
    snowplow__ad_views_passthroughs: [
      \"v_collector\",
      {\"sql\": \"v_tracker || app_id\", \"alias\": \"tracker_app_id\", \"agg\": \"max\"},
      {\"sql\": \"v_tracker || app_id\", \"alias\": \"tracker_app_id_1\", \"agg\": \"min\"},
      {\"sql\": \"v_collector\", \"alias\": \"tracker_app_id_2\", \"agg\": \"min\"}
    ]
  }'" || exit 1;

  eval "dbt test --target $db --select snowplow_media_player_media_ad_views_this_run" || exit 1;

  echo "Snowplow media player integration tests: All tests passed"

done