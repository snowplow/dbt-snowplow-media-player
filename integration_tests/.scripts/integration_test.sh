#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "databricks" "postgres" "redshift" "snowflake")

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

  echo "Snowplow media player integration tests: Execute models - run 1/6"

  eval "dbt run --target $db --full-refresh --vars '{snowplow__allow_refresh: true}'" || exit 1;

  for i in {2..3}
  do
    echo "Snowplow media player integration tests: Execute models - run $i/6"

    eval "dbt run --target $db" || exit 1;
  done

  for i in {4..6}
  do
    echo "Snowplow media player integration tests: Execute models - run $i/6"

    eval "dbt run --target $db --vars '{snowplow__backfill_limit_days: 300}'" || exit 1;
  done

  echo "Snowplow media player integration tests: Test models"

  eval "dbt test --target $db" || exit 1;

  echo "Snowplow media player integration tests: All tests passed"

done
