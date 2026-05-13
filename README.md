<img src="docs/images/radar-logo.svg" width="240" alt="logo" style="padding-top: 48px"/>

# Xonai Compatibility Radar

Xonai Compatibility Radar reads your Apache Spark event logs and tells you how much of your workload the [Xonai Apache Spark Accelerator](https://xonai.io/) supports. Use it to decide which workload is the best to start with.

## How it works

Radar parses the physical execution plan from your event work logs by analysing the operators Spark that *actually* ran, not just the queries you wrote. It checks each operator and expression against what Xonai Accelerator supports and calculates how much of your SQL task time is covered. 

Radar ships as a self-contained JAR and only reads event logs you point to. It does not access your data, connect to your cluster, or make any external calls. 

Report is typically ready in seconds.

→ [Download the latest release](https://github.com/xonai-computing/xonai-compatibility-radar/releases) · Email the result at [radar@xonai.io](mailto:radar@xonai.io)

## Requirements

- Java 8 or later
- Spark event logs from Spark 3.x and Spark 4.x applications
    - Supported sources: Apache Spark OSS and Amazon EMR

> NOTE: To ensure the event logs contain the information required by the tool, Spark applications must be run with the configuration `--conf spark.sql.ui.explainMode=formatted`.

## Getting Started

Install and extract the tool:

```bash
wget https://github.com/xonai-computing/xonai-compatibility-radar/releases/download/v1.0.0/xonai-radar-1.0.0.tar.gz
tar -xzvf xonai-radar-1.0.0.tar.gz
```

Verify the installation:

```bash
./xonai-radar --help
```

## Usage

Point Radar at your event logs and it will analyse them and print the report to the terminal. 

```bash
./xonai-radar -i <event-log-path> -o <report-path> [options]
```

`-i, --input <path>`  points to event log path(s) - file, directory, or comma-separated list.

`-o, --output <path>` points to a directory where results are written.

> NOTE: For results that give you confidence to act on, use logs from workloads that are representative of what runs in production. Development logs with simplified queries may understate coverage.

### Options

| Option | Description |
| --- | --- |
| `--output-format <fmt>` | Output format: `txt` (default) or `csv` |
| `--show-errors` | Print physical plan parsing errors |
| `--overwrite-output` | Overwrite existing result files within the specified path |
| `--help` | Print help information |

Input and output paths support local filesystem, HDFS, Amazon S3, and Google Cloud Storage.

### Cloud Storage Authentication

Only needed if your event logs or output are on AWS or GCS:

**Amazon S3**

- With IAM access: credentials are picked up automatically from the EC2 instance attached to the profile.
- Without IAM access: set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`;

**Google Cloud Storage**

- On GCE: application default credentials are picked up automatically.
- Without a service account: set `GCS_SERVICE_ACCOUNT_JSON_KEYFILE` to the path of a service account JSON key file;

### Running in Docker (optional)

Radar can run inside a Docker container with network access fully disabled; useful if your security policy requires isolated execution. 

Use `--network none` to disable all network access inside the container. Combined with a read-only mount for the input directory, nothing can leave your infrastructure.

## Output

Radar produces two files, and a third only when parsing errors occur.

| File                   | Description                                                                          |
|------------------------|--------------------------------------------------------------------------------------|
| `applications_summary` | How much of each application's task time Xonai Accelerator supports                  |
| `sql_support`          | Support status for each operator and expression, and how much task time each carries |
| `sql_parse_errors`     | Physical plan parsing errors - only created when errors are present                  |

### Applications Summary File

One row per Spark application.

| Column                                                  | Description                                                                                                                  |
|---------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `Application Id`                                        | Spark application identifier                                                                                                 |
| `Application Name`                                      | Spark application name                                                                                                       |
| `Spark Version`                                         | Spark version the application ran on                                                                                         |
| `Complete`                                              | Application completion status; Results may be inaccurate if the logs were collected before the application finished running. |
| `Total Task Time`                                       | Total CPU time across all tasks                                                                                              |
| `SQL Task Time`                                         | CPU time spent inside SQL/DataFrame operations.                                                                              |
| `Supported Task Time`                                   | CPU time that the Xonai Accelerator supports. Set to `ERROR` if the application finished with errors.                        |
| `Coverage`                                              | Percentage of total task time the Xonai Accelerator supports. Set to `ERROR` if the application finished with errors.        |

Example:

```text
+----------------+------------------+---------------+----------+-----------------+---------------+---------------------+----------+
| Application Id | Application Name | Spark Version | Complete | Total Task Time | SQL Task Time | Supported Task Time | Coverage |
+----------------+------------------+---------------+----------+-----------------+---------------+---------------------+----------+
| 1752001041750  | DailyStats       | 3.5.8         | Yes      |     33 min 30 s |   30 min 29 s |         28 min 19 s | 84%      |
+----------------+------------------+---------------+----------+-----------------+---------------+---------------------+----------+
```

### SQL Support File

One row per SQL node type per application. It shows which operators and expressions are supported and how much task time each carries.

A node type may appear multiple times if support varies across query structures. 

| Column             | Description                                                                                |
|--------------------|--------------------------------------------------------------------------------------------|
| `Application Id`   | Spark application identifier                                                               |
| `SQL Node Name`    | Physical plan node type - operator or expression                                           |
| `Support`          | Support status with the Xonai Accelerator - see values below                               |
| `Impact Task Time` | Total task time in stages where this node type appears at least once                       |
| `Node Count`       | Number of times this operator or expression appeared across all queries in the application |

**Support values:**

| Value                 | Meaning                                                                                |
|-----------------------|----------------------------------------------------------------------------------------|
| `Supported`           | Compatible with the Xonai Accelerator                                                  |
| `NotImplemented`      | No support exists for this node                                                        |
| `UnsupportedDataType` | Some support exists, but not for the data types used in your workload                  |
| `UndefinedDataType`   | The physical plan parser could not determine the actual data types                     |
| `Unknown`             | This node is not known to Xonai - likely a recently added Spark expression or operator |

Example:

```text
+----------------+-----------------------+---------------------+------------------+------------+
| Application Id | SQL Node Name         | Support             | Impact Task Time | Node Count |
+----------------+-----------------------+---------------------+------------------+------------+
| 1752001041750  | AttributeReference    | Supported           |      33 min 29 s | 100        |
| 1752001041750  | ShuffleExchangeExec   | Supported           |      33 min 29 s | 8          |
| 1752001041750  | WholeStageCodegenExec | Supported           |      33 min 29 s | 18         |
| 1752001041750  | InputAdapterExec      | Supported           |      33 min 29 s | 23         |
| 1752001041750  | ProjectExec           | Supported           |      19 min 23 s | 8          |
| 1752001041750  | SortExec              | Supported           |      18 min 10 s | 7          |
| 1752001041750  | SortOrder             | Supported           |      18 min 10 s | 9          |
| 1752001041750  | SortMergeJoinExec     | Supported           |      18 min 10 s | 3          |
| 1752001041750  | FilterExec            | Supported           |      15 min 19 s | 7          |
| 1752001041750  | ColumnarToRowExec     | Supported           |      15 min 19 s | 7          |
| 1752001041750  | FileSourceScanExec    | Supported           |      15 min 19 s | 7          |
| 1752001041750  | And                   | Supported           |      15 min 19 s | 13         |
| 1752001041750  | IsNotNull             | Supported           |      15 min 19 s | 15         |
| 1752001041750  | Literal               | Supported           |       6 min 23 s | 14         |
| 1752001041750  | AggregateExpression   | Supported           |       5 min 10 s | 4          |
| 1752001041750  | HashAggregateExec     | UnsupportedDataType |       5 min 10 s | 2          |
| 1752001041750  | Multiply              | Supported           |       5 min 10 s | 2          |
| 1752001041750  | ShuffleExchangeExec   | Supported           |       5 min 10 s | 1          |
| 1752001041750  | Sum                   | Supported           |       5 min 10 s | 2          |
| 1752001041750  | Subtract              | Supported           |       5 min 10 s | 6          |
| 1752001041750  | BroadcastHashJoinExec | Supported           |       5 min 10 s | 2          |
| 1752001041750  | LessThan              | Supported           |       1 min 13 s | 1          |
| 1752001041750  | GreaterThanOrEqual    | Supported           |       1 min 13 s | 1          |
+----------------+-----------------------+---------------------+------------------+------------+
```

### Interpreting Coverage

Coverage is a conservative estimate - true coverage can be higher. 

If any operator in a pipeline stage is unsupported, the entire stage is excluded from Supported Task Time - even if every other operator in that stage is supported.

This is because event logs record the total runtime of each stage, but not how that time is distributed across individual operators within it.

### Share the Report

Send your results to our engineering team - they'll map the results to your specific workload and tell you what's worth accelerating.

→ [radar@xonai.io](mailto:radar@xonai.io)

## Build from Source

The source code is fully open and auditable. If you want to inspect it before running, clone the repo and build it yourself.

Requirements:

- Java 8 or later
- Maven 3.x
- Spark event logs from Spark 3.x CPU applications
    - Supported sources: Apache Spark OSS and Amazon EMR

```
git clone https://github.com/xonai-computing/xonai-compatibility-radar.git
cd xonai-compatibility-radar
./dev/package.sh
```

The package is produced at `target/xonai-radar-<version>.tar.gz`. 

From there, follow the [Getting Started](#getting-started) steps to run it.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
