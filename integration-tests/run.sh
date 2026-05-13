#!/bin/bash

#
# Copyright 2026 XONAI LTD
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

RADAR_HOME="${1:-"$(pwd)/target"}"

INPUT_FOLDER="$(pwd)/integration-tests/event-logs"
OUTPUT_FOLDER="$(pwd)/target/integration-tests"

mkdir -p "${OUTPUT_FOLDER}"

CMD="${RADAR_HOME}/xonai-radar -i ${INPUT_FOLDER} -o ${OUTPUT_FOLDER}"
echo "${CMD}"
$CMD

assert_file_exists() {
  if [[ ! -f "$1" ]]; then
    echo "ERROR: Expected file does not exist - $1"
    exit 1
  fi
}

assert_file_exists "${OUTPUT_FOLDER}/applications_summary.txt"
assert_file_exists "${OUTPUT_FOLDER}/sql_support.txt"
assert_file_exists "${OUTPUT_FOLDER}/sql_parse_errors.txt"

ERROR_APPS=$(cat "${OUTPUT_FOLDER}/applications_summary.txt" | grep ERROR)
ERROR_APPS_COUNT=$(cat "${OUTPUT_FOLDER}/applications_summary.txt" | grep ERROR | wc -l)

if [[ "${ERROR_APPS_COUNT}" != "1" ]]; then
  echo "ERROR: Expected 1 application with errors got ${ERROR_APPS_COUNT}"
  exit 1
fi

ERROR_248=$(cat "${OUTPUT_FOLDER}/applications_summary.txt" | grep ERROR | grep "2.4.8" | wc -l)
if [[ "${ERROR_248}" != "1" ]]; then
  echo "ERROR: Expected Spark 2.4.8 application to have error got:"
  echo "${ERROR_APPS}"
  exit 1
fi
