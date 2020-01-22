<p align="right">
<a href="https://autorelease.general.dmz.palantir.tech/palantir/spark-tpcds-benchmark"><img src="https://img.shields.io/badge/Perform%20an-Autorelease-success.svg" alt="Autorelease"></a>
</p>

TPCDS Spark Benchmark Runner
==================================

TPC-DS is an open benchmark suite for structured data systems. This utility aims to make it easy to generate TPC-DS
data, and to run TPC-DS benchmarks against different versions of Spark. The main use case for this utility is to run
scale tests when developing Spark itself. For example, one can use this tool to check the performance of a change made
to the way Spark shuffle works.

The benchmarks run SQL queries against structured datasets. This utility is thus not useful for running tests in
streaming workflows.

The benchmark suite can be run on MacOS or CentOS 6+. It does not currently support running on Windows.

# Usage

The benchmark suite requires a distributed storage layer to put the generated test data as well as the computation
results. This tool also requires a cluster manager for running the Spark driver and executors.

1. Run `./gradlew distTar` to build the initial distribution.
2. Get the distribution from `spark-tpcds-benchmark-runner/build/distributions/spark-tpcds-benchmark-runner-<VERSION>.tgz`
3. Upload and unpack the distribution to a node in the cluster.
4. In the distribution, edit `var/conf/config.yml` to match the benchmarking environment you will run with.
   The configuration schema is described below.
5. Run `service/bin/init.sh start`. The benchmarks will begin running in the background. The driver exits upon
   completing the benchmark suite.

Your configuration file contains a field called `testDataDir` (see below). The source tables are stored under `<testDataDir>/tpcds_data`.
The performance results of running the benchmarks can be found in JSON files located under `<testDataDir>/tpcds_benchmark_results`.
You may use a Spark shell to load these JSON files into DataFrames for additional analysis, or download these JSON files
for processing by other tools. Results are grouped by data scale defined by the configuration's `dataScalesGb`, described below.

The correctness of the computation is checked against the results of previous executions of the benchmark against the
same set of data. If the source data is regenerated and the previous source data is overwritten, the computation results
from previous runs are also invalidated.

# Configuration Schema

The schema for the configuration file is described below. Note that all fields are mandatory, so there are no defaults.
You may use the configuration file provided in the repository as a template to fill in some reasonable defaults.

## Top Level File

| Property Name              | Property Type        | Property Description                                     |
| ---------------------------|----------------------|----------------------------------------------------------|
| `spark`                    | `SparkConfiguration` | Spark properties. See Spark Configuration schema below.  |
| `hadoop`                   | `HadoopConfiguration`| Hadoop properties. See Hadoop configuration schema below.|
| `dsdgenWorkLocalDir`       | `Path`               | Path to use as scratch space for generating TPC-DS data. |
| `testDataDir`              | `URI`                | Fully qualified URI for the place to put both the test data and the experiment computation results. Should be a URI that can be managed by an implementation of the Hadoop FileSystem API. |
| `generateData`             | `boolean`            | Whether or not to generate data on startup before running benchmarks.|
| `overwriteData`            | `boolean`            | Whether or not to overwrite generated data on startup, if `generateData` is set to true. This will also invalidate the computation results that are used to check for the correctness of a benchmark run.|
| `dataScalesGb`             | `list<integer>`      | Data scales to run benchmarks, and generate data, on. Data scales are in GB. The entire benchmark suite is run once per data scale listed here.|
| `dataGenerationParallelism`| `integer`            | Number of threads to use for generating benchmark data.|
| `iterations`               | `integer`            | Number of iterations of the benchmarks to run.         |

## Spark Configuration

Here's the schema for the `spark` field described above.

| Property Name              | Property Type        | Property Description                                     |
| ---------------------------|----------------------|----------------------------------------------------------|
| `master`                   | `string`             | Spark master to connect to. Translates to the `spark.master` Spark property. Note that all benchmarks run in client mode.|
| `executorInstances`        | `integer`            | Number of Spark executors to use. Translates to the `spark.executor.instances` Spark property.|
| `executorCores`            | `integer`            | Number of cores to allocate to each executor. Translates to the `spark.executor.cores` Spark property.|
| `executorMemory`           | `string`             | Amount of memory to allocate to each executor. Translates to the `spark.executor.memory` Spark property. Specified as a memory String that can be interpreted as a heap size by the Java runtime (e.g. `8g`, `256m`, etc.)|
| `sparkConf`                | `map<string, string>`| Additional Spark configuration parameters to apply for this benchmark run.|

## Hadoop Configuration

Here's the schema for the `hadoop` field given in the top level file.

| Property Name              | Property Type        | Property Description                                     |
| ---------------------------|----------------------|----------------------------------------------------------|
| `hadoopConfDirs`           | `list<path>`         | List of directories containing Hadoop configuration XML files to load Hadoop configurations from.|
| `hadoopConf`               | `map<string, string>`| Additional Hadoop configuration parameters to apply for this benchmark run.|
