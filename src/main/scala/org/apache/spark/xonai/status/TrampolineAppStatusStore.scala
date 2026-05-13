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

package org.apache.spark.xonai.status

import com.xonai.spark.sql.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo, JobData, StageData}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.util.kvstore.KVStore

/**
 * [[org.apache.spark.status.AppStatusStore]].
 */
class TrampolineAppStatusStore(store: KVStore) {

  val appStatusStore = new AppStatusStore(store)

  def applicationInfo(): ApplicationInfo = {
    val app = appStatusStore.applicationInfo()
    new ApplicationInfo(
      app.id,
      app.name,
      app.coresGranted,
      app.maxCores,
      app.coresPerExecutor,
      app.memoryPerExecutorMB,
      app.attempts.map { attempt =>
        new ApplicationAttemptInfo(
          attempt.attemptId,
          attempt.startTime,
          attempt.endTime,
          attempt.lastUpdated,
          attempt.duration,
          attempt.sparkUser,
          attempt.completed,
          attempt.appSparkVersion
        )
      }
    )
  }

  def jobsList(): Seq[JobData] = {
    appStatusStore.jobsList(null).map { job =>
      new JobData(
        job.jobId,
        job.name,
        job.description,
        job.submissionTime,
        job.completionTime,
        job.stageIds,
        job.jobGroup,
        job.status,
        job.numTasks,
        job.numActiveTasks,
        job.numCompletedTasks,
        job.numSkippedTasks,
        job.numFailedTasks,
        job.numKilledTasks,
        job.numCompletedIndices,
        job.numActiveStages,
        job.numCompletedStages,
        job.numSkippedStages,
        job.numFailedStages,
        job.killedTasksSummary
      )
    }
  }

  def stageData(stageId: Int): Seq[StageData] = {
    appStatusStore.stageData(stageId).map { stage =>
      new StageData(
        stage.status,
        stage.stageId,
        stage.attemptId,
        stage.numTasks,
        stage.numActiveTasks,
        stage.numCompleteTasks,
        stage.numFailedTasks,
        stage.numKilledTasks,
        stage.numCompletedIndices,
        stage.submissionTime,
        stage.firstTaskLaunchedTime,
        stage.completionTime,
        stage.failureReason,
        stage.executorDeserializeTime,
        stage.executorDeserializeCpuTime,
        stage.executorRunTime,
        stage.executorCpuTime,
        stage.resultSize,
        stage.jvmGcTime,
        stage.resultSerializationTime,
        stage.memoryBytesSpilled,
        stage.diskBytesSpilled,
        stage.peakExecutionMemory,
        stage.inputBytes,
        stage.inputRecords,
        stage.outputBytes,
        stage.outputRecords,
        stage.shuffleRemoteBlocksFetched,
        stage.shuffleLocalBlocksFetched,
        stage.shuffleFetchWaitTime,
        stage.shuffleRemoteBytesRead,
        stage.shuffleRemoteBytesReadToDisk,
        stage.shuffleLocalBytesRead,
        stage.shuffleReadBytes,
        stage.shuffleReadRecords,
        stage.shuffleWriteBytes,
        stage.shuffleWriteTime,
        stage.shuffleWriteRecords,
        stage.name,
        stage.description,
        stage.details,
        stage.schedulingPool,
        stage.rddIds
      )
    }
  }
}
