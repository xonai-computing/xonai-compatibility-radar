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

#
# Searches for LICENSE files inside the JAR of all compile dependencies and copies them.
#

set -eo pipefail

WORKING_DIR="/tmp/xonai-dev"

mkdir -p licenses

exclude_notice_if_exists() {
  if [[ -f "${WORKING_DIR}/META-INF/NOTICE" ]]; then
    echo "    <exclude>META-INF/NOTICE</exclude>"
  fi
}

copy_license_if_exists() {
  local fileName=$1
  local sourceFile=$2
  local sourcePath="${WORKING_DIR}/${sourceFile}"
  local targetFile="${artifactId}-${fileName}"
  local targetPath="./licenses/${targetFile}"
  if [[ -f $sourcePath ]]; then
    if [[ -f $targetPath ]]; then
      echo "Already exists - ${targetFile}"
    else
      cp $sourcePath $targetPath
      echo "<filter>"
      echo "  <artifact>${groupId}:${artifactId}</artifact>"
      echo "  <excludes>"
      echo "    <!-- Handled by prepare-licenses.sh -->"
      echo "    <exclude>${sourceFile}</exclude>"
      exclude_notice_if_exists
      echo "  </excludes>"
      echo "</filter>"
    fi
  fi
}

# For each compile dependency.
for line in $(mvn dependency:tree -Ddetail=true | grep :compile); do
  if [[ "$line" == *:compile ]]; then
    groupId=$(echo "$line" | sed "s/:.*//")
    artifactId=$(echo "$line" | sed "s/:jar.*//" | sed "s/^.*://")
    version=$(echo "$line" | sed "s/:compile$//" | sed "s/^.*://")

    groupIdPath=$(echo "${groupId}" | sed "s/\./\//g")
    jar="${HOME}/.m2/repository/${groupIdPath}/${artifactId}/${version}/${artifactId}-${version}.jar"

    # Create temporary directory and extract JAR.
    rm -r -f ${WORKING_DIR}
    mkdir -p ${WORKING_DIR}
    unzip -q -d ${WORKING_DIR} $jar

    # Copy LICENSE if exists.
    copy_license_if_exists "LICENSE" "LICENSE"
    copy_license_if_exists "LICENSE" "META-INF/LICENSE"
    copy_license_if_exists "LICENSE.md" "META-INF/LICENSE.md"
    copy_license_if_exists "LICENSE.txt" "META-INF/LICENSE.txt"
    copy_license_if_exists "ASL2.0" "META-INF/ASL2.0"
    copy_license_if_exists "bigint-LICENSE" "META-INF/bigint-LICENSE"
    copy_license_if_exists "FastDoubleParser-LICENSE" "META-INF/FastDoubleParser-LICENSE"
    copy_license_if_exists "LICENSE.aix-netbsd.txt" "META-INF/license/LICENSE.aix-netbsd.txt"
    copy_license_if_exists "LICENSE.boringssl.txt" "META-INF/license/LICENSE.boringssl.txt"
    copy_license_if_exists "LICENSE.mvn-wrapper.txt" "META-INF/license/LICENSE.mvn-wrapper.txt"
    copy_license_if_exists "LICENSE.tomcat-native.txt" "META-INF/license/LICENSE.tomcat-native.txt"
    copy_license_if_exists "LICENSE.protobuf.txt" "META-INF/licenses-binary/LICENSE.protobuf.txt"

    # Cleanup temporary directory.
    rm -r ${WORKING_DIR}
  fi
done
