

<p align="right">
<a href="https://autorelease.general.dmz.palantir.tech/palantir/spark-tpcds-benchmark"><img src="https://img.shields.io/badge/Perform%20an-Autorelease-success.svg" alt="Autorelease"></a>
</p>

Spark Benchmark Runner
======================

This repo contains tools to run 2 industry standard benchmark suites:

1. [TPC-DS](http://www.tpc.org/tpcds/) is an open benchmark suite for structured data systems. This utility aims to make it easy to generate TPC-DS data, and to run TPC-DS benchmarks against different versions of Spark. The main use case for this utility is to test performance at scale when evaluating changes to Spark or to its underlying infrastructure. The benchmarks run SQL queries against structured datasets. This utility is thus not useful for running tests in streaming workflows.

2. [Sort Benchmark](http://sortbenchmark.org/): This is a single benchmark that sorts a large amount of data generated by the [gensort](http://www.ordinal.com/gensort.html) program.

The benchmark suite can be run on MacOS or CentOS 6+. It does not currently support running on Windows.

# Usage

The benchmark suite requires a storage layer, distributed (such as HDFS/S3/Azure Blob Storage) or local to store the generated test data, as well as the computation results. This tool supports running the benchmarks either in local spark mode on a single JVM, or with a cluster manager, such as YARN when running distributed benchmarks on several machines.

 - Run `./gradlew distTar` to build the initial distribution.
 - Get the distribution from `spark-tpcds-benchmark-runner/build/distributions/spark-tpcds-benchmark-runner-<VERSION>.tgz`
 - Upload and unpack the distribution to a node in the cluster.
 - In the distribution, edit `var/conf/config.yml` to match the benchmarking environment you will run with. **Documentation for the various configurable options are described in the [config.yml](https://github.com/palantir/spark-tpcds-benchmark/blob/develop/spark-tpcds-benchmark-runner/var/conf/config.yml) file.**
	- Storage Layer:
		- This tool supports any Hadoop compatible storage layer (eg S3/ABS/HDFS). Once that is setup, the credentials and account details can be updated in the `hadoop` configuration section. Placeholder configuration blocks are provided for S3, ABS and HDFS.
	- Compute Layer:
		 - When running with local spark, the `spark` configuration section in config.yml should work out of the box.
		 - When running on a cluster manager, the cluster first needs to be installed and configured. If you use YARN, [this](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html#:~:text=core%2Ddefault.xml-,Apache%20Hadoop%20YARN,or%20a%20DAG%20of%20jobs.) and [this](http://spark.apache.org/docs/latest/running-on-yarn.html) are good places to start. Once that is done, the `spark` and `hadoop` configuration sections need to be changed to point to the cluster manager.
	 - Ephemeral Disks
		 - We recommend setting `hadoop.tmp.dir` to a fast SSD drive for each machine. It is set to a subfolder in `/scratch` by default.
		 - On AWS, we typically use m5d/r5d instance types, which already come with  NVMe SSD ephemeral disks, but are not mounted anywhere. We use this [script](https://gist.github.com/rahij/c7e84c5e0ec06e025873b09882123654) to mount it to `scratch`. These already come with hardware level encryption, so no LUKS encryption is necessary.
		 - On Azure, we typically use hc44rs or d48ds_v4 instance types. These come with SSD ephemeral disks and aren't mounted either. They also do not have hardware level encryption as of the time of writing (July 2020). We use this [script](https://gist.github.com/rahij/c47537999f87e486e1465ee27f20b895) with `CLOUD_PROVIDER=azure` to mount and LUKS encrypt them.
 - Set the JAVA_HOME environment variable to point to Java 11.
 - Run `service/bin/init.sh start`. The benchmarks will begin running in the background. The driver exits upon
   completing the benchmark suite.

The performance results of running the benchmarks can be found in JSON files located under `benchmark_results/` in the specified metrics filesystem.
You may use a Spark shell to load these JSON files into DataFrames for additional analysis, or download these JSON files
for processing by other tools. Results are grouped by data scale defined by the configuration's `dataScalesGb`, described below.

For TPC-DS, the correctness of the computation is checked against the results of previous executions of the benchmark against the
same set of data. If the source data is regenerated and the previous source data is overwritten, the computation results
from previous runs are also invalidated.
