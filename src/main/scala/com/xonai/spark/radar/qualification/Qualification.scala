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

package com.xonai.spark.radar.qualification

import com.xonai.hadoop.conf.ConfigurationUtils
import com.xonai.spark.radar.Radar
import com.xonai.spark.sql.status.ExtendedSQLAppStatusListener
import com.xonai.spark.sql.support.XonaiSupport
import com.xonai.utils.Utils
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.xonai.{HistoryProvider, Trampoline}

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.util.logging.{Level, LogManager}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Entry point for Xonai Qualification.
 */
object Qualification {

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[QualificationOptions]("xonai-radar") {
      note("Create Xonai compatibility report using Spark event logs.\n")

      opt[String]('i', "input")
        .valueName("<path>")
        .action { (x, options) =>
          val paths = x.split(',').filter(_.nonEmpty)
          options.copy(inputPaths = paths.toSeq)
        }
        .text("event log paths, directory or file")
        .required()

      opt[String]('o', "output")
        .valueName("<path>")
        .action { (x, options) =>
          val path =
            if (x.startsWith("s3")) {
              x.stripSuffix("/").concat("/")
            } else {
              x
            }
          options.copy(outputPath = path)
        }
        .text("directory where to write the results")
        .required()

      opt[String]("output-format")
        .validate { x =>
          val valid = Set("txt", "csv")
          if (valid.contains(x)) {
            success
          } else {
            failure(s"'--output-format' must be one of: ${valid.mkString(", ")}")
          }
        }
        .action { (x, options) => options.copy(outputFormat = x) }
        .text("file format of the results - 'txt' (default) or 'csv'")

      opt[Unit]("show-errors")
        .action { (_, options) =>
          options.copy(showErrors = true)
        }
        .text("print physical plan parsing errors")

      opt[Unit]("overwrite-output")
        .action { (_, options) =>
          options.copy(overwriteOutput = true)
        }
        .text("overwrite result files if they exist")

      help("help").text("print this help and exit")

      note("\nSupported file stores:")
      note("  - Local file system")
      note("  - HDFS")
      note("  - Amazon S3")
      note("  - Google Cloud Storage")

      note("\nExample:")
      note("  ./xonai-radar -i file://eventlogs -o file://report")

      checkConfig { options =>
        val pathPrefixes = Set("s3://", "s3a://", "gs://", "file:/", "hdfs://", "/")
        if (options.inputPaths.exists(p => !pathPrefixes.exists(p.startsWith))) {
          failure(s"'--input' must be prefixed by one of: ${pathPrefixes.mkString(", ")}")
        } else if (!pathPrefixes.exists(options.outputPath.startsWith)) {
          failure(s"'--output' must be prefixed by one of: ${pathPrefixes.mkString(", ")}")
        } else {
          success
        }
      }
    }

    val defaultOptions = QualificationOptions(
      inputPaths = Seq(),
      outputPath = "",
      outputFormat = "txt",
      engine = "xonai",
      showErrors = false,
      overwriteOutput = false
    )

    // Reduce noise from loggers using the Java API.
    Option(LogManager.getLogManager.getLogger("")).foreach(_.setLevel(Level.WARNING))

    parser.parse(args, defaultOptions) match {
      case Some(options) =>
        val main = new QualificationMain(options)
        if (main.validate()) {
          Radar.printHeader()
          val result = main.qualify()
          System.exit(result)
        } else {
          parser.showTryHelp()
        }
      case None =>
        System.exit(1)
    }
  }
}

/**
 * Orchestrates command line logic for Xonai Qualification.
 */
class QualificationMain(options: QualificationOptions) extends Logging {

  private var failed: Boolean = false

  private var errorsWriter: Option[PrintWriter] = None

  private var applications = new ArrayBuffer[QualificationApplicationSummary]()

  private val sqlNodeSupport = new ArrayBuffer[QualificationSQLNodeSupport]()

  private val qualifier = new Qualifier(
    options.engine match {
      case "xonai" => new XonaiSupport()
    }
  )

  def validate(): Boolean = {
    lazy val inputPathsAreValid =
      options.inputPaths.forall { pathStr =>
        val path = new Path(pathStr)
        val fileSystem = getFileSystem(path)
        if (fileSystem.isFailure) {
          false
        } else {
          validatePathExists(path, fileSystem.get)
        }
      }

    lazy val outputPathIsValid = {
      val pathStr = options.outputPath
      val path = new Path(pathStr)
      val fileSystem = getFileSystem(path)
      if (fileSystem.isFailure) {
        false
      } else {
        val exists = validatePathExists(path, fileSystem.get)
        if (exists) {
          val isDirectory = fileSystem.get.getFileStatus(path).isDirectory
          if (!isDirectory) {
            logError(s"Output path '$pathStr' is not a directory")
          }
          isDirectory &&
          validateOutputPath(getApplicationSummaryFilePath, fileSystem.get) &&
          validateOutputPath(getSQLSupportFilePath, fileSystem.get) &&
          validateOutputPath(getErrorsFilePath, fileSystem.get)
        } else {
          false
        }
      }
    }

    inputPathsAreValid && outputPathIsValid
  }

  def qualify(): Int = {
    // Load applications from event logs and process them.
    processEventLogs()
    errorsWriter.foreach(_.close())

    // Sort by decreasing supported time.
    applications = applications.sortBy(-_.supportedTaskTime.getOrElse(0L))

    // Write summary files.
    writeSQLSupportFile()
    writeApplicationsSummaryFile()

    // Print summary to stdout.
    printSummary()

    if (failed) 1 else 0
  }

  private def processEventLogs(): Unit = {
    options.inputPaths.foreach { pathStr =>
      val path = new Path(pathStr)
      val fileSystem = getFileSystem(path).get
      val files = fileSystem.listStatus(path)
      files.foreach { file =>
        processEventLogs(fileSystem, file.getPath)
      }
    }
  }

  private def processEventLogs(fs: FileSystem, path: Path): Unit = {
    val historyProvider = new HistoryProvider(fs, path)
    val extendedListener = new ExtendedSQLAppStatusListener(historyProvider.store)
    historyProvider.addListener(extendedListener)

    val result = historyProvider.rebuild { kvstore =>
      val store = new QualificationStore(kvstore, extendedListener)
      val application = store.getApplication
      handleApplication(application)
    }

    if (result.isEmpty) {
      logError(s"Skipping file/directory not recognized as Spark event logs: '$path'")
      failed = true
    }
  }

  private def handleApplication(application: QualificationApplication): Unit = {
    qualifier.tag(application)

    val applicationSummary = qualifier.summarize(application)
    applications += applicationSummary

    val nodeSupport = qualifier.getNodeSupport(application).sortBy(-_.impactTaskTime)
    sqlNodeSupport ++= nodeSupport

    writeErrorsToFile(application)
  }

  private def writeErrorsToFile(application: QualificationApplication): Unit = {
    val queriesWithErrors = application.sqlQueries.filter(_.physicalPlan.isFailure)
    if (queriesWithErrors.isEmpty) {
      return
    }
    failed = true

    if (options.showErrors) {
      queriesWithErrors.foreach { query =>
        query.physicalPlan.failed.foreach { exception =>
          logError(
            s"Failed to parse physical plan of SQL query '${query.queryId}' in application " +
              s"'${application.applicationId}'.",
            exception
          )
        }
      }
    }

    // Prepare file.
    if (errorsWriter.isEmpty) {
      val path = getOutputPath("sql_parse_errors.txt")
      val writer = initOutputFile(path)
      if (writer == null) {
        failed = true
        return
      }

      errorsWriter = Some(writer)
    }

    val writer = errorsWriter.get
    queriesWithErrors.foreach { query =>
      writer.println("---------------")
      writer.println("Application id: " + application.applicationId)
      writer.println("SQL query id: " + query.queryId)
      writer.println("---------------")
      query.physicalPlan.failed.get.printStackTrace(writer)
    }
  }

  private def writeSQLSupportFile(): Unit = {
    // Prepare file.
    val writer = initOutputFile(getSQLSupportFilePath)
    if (writer == null) {
      failed = true
      return
    }

    // Write contents.
    if (options.outputFormat == "csv") {
      val header =
        Seq(
          "application_id",
          "sql_node_name",
          "support",
          "impact_task_time_seconds",
          "node_count"
        ).mkString(",")
      writer.println(header)

      sqlNodeSupport.foreach { support =>
        val line = Seq(
          support.applicationId,
          support.nodeName,
          support.support,
          support.impactTaskTime / 1000,
          support.nodeCount
        ).mkString(",")

        writer.println(line)
      }
    } else {
      val header = Seq(
        "Application Id",
        "SQL Node Name",
        "Support",
        "Impact Task Time",
        "Node Count"
      )
      val rows = sqlNodeSupport.map { support =>
        Seq(
          support.applicationId,
          support.nodeName,
          support.support,
          Utils.formatDuration(support.impactTaskTime),
          support.nodeCount.toString
        )
      }
      val table =
        Utils.tableString((header +: rows).toSeq, maxColumnWidth = Int.MaxValue, leftPad = Set(3))
      writer.println(table)
    }

    writer.close()
  }

  private def writeApplicationsSummaryFile(): Unit = {
    // Prepare file.
    val writer = initOutputFile(getApplicationSummaryFilePath)
    if (writer == null) {
      failed = true
      return
    }

    // Write contents.
    if (options.outputFormat == "csv") {
      val header =
        Seq(
          "application_id",
          "application_name",
          "spark_version",
          "complete",
          "total_task_time_seconds",
          "sql_task_time_seconds",
          "supported_task_time_seconds",
          "coverage"
        ).mkString(",")
      writer.println(header)

      applications.foreach { application =>
        val line = Seq(
          application.id,
          application.name,
          application.sparkVersion,
          application.completed,
          application.totalTaskTime / 1000,
          application.sqlTaskTime / 1000,
          application.supportedTaskTime.map(_ / 1000).getOrElse("ERROR"),
          application
            .supportedTaskTime
            .map(formatPercentage(_, application.totalTaskTime))
            .getOrElse("ERROR")
        ).mkString(",")

        writer.println(line)
      }
    } else {
      val header = Seq(
        "Application Id",
        "Application Name",
        "Spark Version",
        "Complete",
        "Total Task Time",
        "SQL Task Time",
        "Supported Task Time",
        "Coverage"
      )
      val rows = applications.map { application =>
        Seq(
          application.id,
          application.name,
          application.sparkVersion,
          if (application.completed) "Yes" else "No",
          Utils.formatDuration(application.totalTaskTime),
          Utils.formatDuration(application.sqlTaskTime),
          application
            .supportedTaskTime
            .map(Utils.formatDuration)
            .getOrElse("ERROR"),
          application
            .supportedTaskTime
            .map(formatPercentage(_, application.totalTaskTime))
            .getOrElse("ERROR")
        )
      }
      val table = Utils.tableString(
        (header +: rows).toSeq,
        maxColumnWidth = Int.MaxValue,
        leftPad = Set(4, 5, 6)
      )
      writer.println(table)
    }

    writer.close()
  }

  private def printSummary(): Unit = {
    val header = Seq(
      "Application Name",
      "Total Task Time",
      "Xonai Coverage"
    )
    val rows = applications.map { application =>
      Seq(
        application.name,
        Utils.formatDuration(application.totalTaskTime),
        application
          .supportedTaskTime
          .map(formatPercentage(_, application.totalTaskTime))
          .getOrElse("ERROR")
      )
    }
    val table = Utils.tableString((header +: rows).toSeq, maxColumnWidth = 25, leftPad = Set(1))

    print(table)
    print(s"total: ${applications.length} processed")
    val failed = applications.count(_.supportedTaskTime.isEmpty)
    if (failed > 0) {
      print(s", $failed failed")
    }
    println()
    println()
    println(s"Report written to ${options.outputPath}")
    println(
      "Now share it with radar@xonai.io for a technical read from the engineer who built the " +
        "accelerator."
    )
  }

  private def initOutputFile(path: Path): PrintWriter = {
    val fileSystem = getFileSystem(path).get

    // The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
    val uri = path.toUri
    val stream =
      if (uri.getScheme == "file" || uri.getScheme == null) {
        new FileOutputStream(uri.getPath)
      } else {
        Trampoline.hadoopUtilCreateFile(fileSystem, path, allowEC = false)
      }

    try {
      val filePermissions = new FsPermission(Integer.parseInt("660", 8).toShort)
      fileSystem.setPermission(path, filePermissions)
      logInfo(s"Writing to output file: '$path'")
      new PrintWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))
    } catch {
      case e: Exception =>
        stream.close()
        logWarning(s"Failed to open output file: '$path'", e)
        null
    }
  }

  private def getApplicationSummaryFilePath: Path = {
    getOutputPath(s"applications_summary.${options.outputFormat}")
  }

  private def getSQLSupportFilePath: Path = {
    getOutputPath(s"sql_support.${options.outputFormat}")
  }

  private def getErrorsFilePath: Path = {
    getOutputPath("sql_parse_errors.txt")
  }

  private def getOutputPath(filename: String): Path = {
    new Path(options.outputPath.stripSuffix("/") + "/" + filename)
  }

  private def validateOutputPath(path: Path, fileSystem: FileSystem): Boolean = {
    if (fileSystem.exists(path)) {
      if (options.overwriteOutput) {
        logWarning(s"Overwriting output file: '$path'")
        if (!fileSystem.delete(path, true)) {
          logError(s"Unable to overwrite output file: '$path'")
          return false
        }
      } else {
        logError(
          s"Output file already exists: '$path'. Re-run with '--overwrite-output' to overwrite."
        )
        return false
      }
    }
    true
  }

  private def getFileSystem(path: Path): Try[FileSystem] = {
    val hadoopConf = ConfigurationUtils.defaultConfiguration
    val fileSystem = Try(path.getFileSystem(hadoopConf))
    if (fileSystem.isFailure) {
      logError("Cannot load filesystem", fileSystem.failed.get)
    }
    fileSystem
  }

  private def validatePathExists(path: Path, fileSystem: FileSystem): Boolean = {
    val exists = Try(fileSystem.exists(path))
    if (exists.isFailure) {
      logError(s"Cannot read path: '${path.toString}'", exists.failed.get)
    } else if (!exists.get) {
      logError(s"Path does not exist: '${path.toString}'")
    }
    exists.getOrElse(false)
  }

  private def formatPercentage(value: Long, total: Long): String = {
    if (total == 0) {
      return "0%"
    }
    val percentage = value.toDouble / total
    s"${(percentage * 100).toLong}%"
  }
}
