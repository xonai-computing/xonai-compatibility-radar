/*
 * Copyright 2026 XONAI LTD
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xonai.hadoop.conf

import org.apache.hadoop.conf.Configuration

object ConfigurationUtils {

  /**
   * Returns a Hadoop configuration with setup for Amazon S3 or Google Cloud Storage (GCS).
   *
   * Amazon S3 uses [[https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html]]
   * with authentication from [[com.amazonaws.auth.EnvironmentVariableCredentialsProvider]] or
   * [[com.amazonaws.auth.InstanceProfileCredentialsProvider]].
   *
   * GCS uses [[http://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/INSTALL.md]]
   * with Compute Engine authentication or Service Account authentication (using environment
   * variable `GCS_SERVICE_ACCOUNT_JSON_KEYFILE`).
   */
  def defaultConfiguration: Configuration = {
    val hadoopConf = new Configuration()

    // Amazon S3.
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
    hadoopConf.set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    // Google Cloud Storage.
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    val keyFile = System.getenv("GCS_SERVICE_ACCOUNT_JSON_KEYFILE")
    if (keyFile != null && keyFile.nonEmpty) {
      hadoopConf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
      hadoopConf.set("fs.gs.auth.service.account.json.keyfile", keyFile)
    }

    hadoopConf
  }
}
