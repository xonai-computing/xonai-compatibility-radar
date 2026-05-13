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

package com.xonai.spark.sql.status.api.v1

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1.StageStatus

import java.util.Date

/**
 * [[org.apache.spark.status.api.v1.ApplicationInfo]].
 */
class ApplicationInfo(
    val id: String,
    val name: String,
    val coresGranted: Option[Int],
    val maxCores: Option[Int],
    val coresPerExecutor: Option[Int],
    val memoryPerExecutorMB: Option[Int],
    val attempts: collection.Seq[ApplicationAttemptInfo]
)

/**
 * [[org.apache.spark.status.api.v1.ApplicationAttemptInfo]].
 */
class ApplicationAttemptInfo(
    val attemptId: Option[String],
    val startTime: Date,
    val endTime: Date,
    val lastUpdated: Date,
    val duration: Long,
    val sparkUser: String,
    val completed: Boolean = false,
    val appSparkVersion: String
)

/**
 * [[org.apache.spark.status.api.v1.JobData]].
 */
class JobData(
    val jobId: Int,
    val name: String,
    val description: Option[String],
    val submissionTime: Option[Date],
    val completionTime: Option[Date],
    val stageIds: collection.Seq[Int],
    val jobGroup: Option[String],
    val status: JobExecutionStatus,
    val numTasks: Int,
    val numActiveTasks: Int,
    val numCompletedTasks: Int,
    val numSkippedTasks: Int,
    val numFailedTasks: Int,
    val numKilledTasks: Int,
    val numCompletedIndices: Int,
    val numActiveStages: Int,
    val numCompletedStages: Int,
    val numSkippedStages: Int,
    val numFailedStages: Int,
    val killedTasksSummary: Map[String, Int]
)

/**
 * [[org.apache.spark.status.api.v1.StageData]].
 */
class StageData(
    val status: StageStatus,
    val stageId: Int,
    val attemptId: Int,
    val numTasks: Int,
    val numActiveTasks: Int,
    val numCompleteTasks: Int,
    val numFailedTasks: Int,
    val numKilledTasks: Int,
    val numCompletedIndices: Int,
    val submissionTime: Option[Date],
    val firstTaskLaunchedTime: Option[Date],
    val completionTime: Option[Date],
    val failureReason: Option[String],
    val executorDeserializeTime: Long,
    val executorDeserializeCpuTime: Long,
    val executorRunTime: Long,
    val executorCpuTime: Long,
    val resultSize: Long,
    val jvmGcTime: Long,
    val resultSerializationTime: Long,
    val memoryBytesSpilled: Long,
    val diskBytesSpilled: Long,
    val peakExecutionMemory: Long,
    val inputBytes: Long,
    val inputRecords: Long,
    val outputBytes: Long,
    val outputRecords: Long,
    val shuffleRemoteBlocksFetched: Long,
    val shuffleLocalBlocksFetched: Long,
    val shuffleFetchWaitTime: Long,
    val shuffleRemoteBytesRead: Long,
    val shuffleRemoteBytesReadToDisk: Long,
    val shuffleLocalBytesRead: Long,
    val shuffleReadBytes: Long,
    val shuffleReadRecords: Long,
    val shuffleWriteBytes: Long,
    val shuffleWriteTime: Long,
    val shuffleWriteRecords: Long,
    val name: String,
    val description: Option[String],
    val details: String,
    val schedulingPool: String,
    val rddIds: collection.Seq[Int]
)
