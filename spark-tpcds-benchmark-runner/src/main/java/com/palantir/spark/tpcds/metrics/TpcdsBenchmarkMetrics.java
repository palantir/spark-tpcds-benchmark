/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.spark.tpcds.metrics;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.palantir.spark.tpcds.config.TpcdsBenchmarkConfig;
import com.palantir.spark.tpcds.immutables.ImmutablesStyle;
import com.palantir.spark.tpcds.paths.TpcdsPaths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.immutables.value.Value;
import scala.collection.JavaConverters;

public final class TpcdsBenchmarkMetrics {

    private final TpcdsBenchmarkConfig config;
    private final List<Row> metrics = Lists.newArrayList();
    private final TpcdsPaths paths;
    private final SparkSession spark;
    private RunningQuery currentRunningQuery;

    public TpcdsBenchmarkMetrics(TpcdsBenchmarkConfig config, TpcdsPaths paths, SparkSession spark) {
        this.config = config;
        this.paths = paths;
        this.spark = spark;
        this.currentRunningQuery = null;
    }

    public void startBenchmark(String queryName, int scale) {
        Preconditions.checkArgument(currentRunningQuery == null, "Can only run one query at a time.");
        currentRunningQuery = RunningQuery.builder()
                .queryName(queryName)
                .scale(scale)
                .timer(Stopwatch.createStarted())
                .build();
    }

    public void stopBenchmark() {
        Preconditions.checkArgument(
                currentRunningQuery != null, "No benchmark is currently running.");
        Stopwatch stopped = currentRunningQuery.timer();
        long endTime = System.currentTimeMillis();
        long elapsed = stopped.elapsed(TimeUnit.MILLISECONDS);
        long startTime = endTime - elapsed;
        metrics.add(TpcdsBenchmarkMetric.builder()
                .queryName(currentRunningQuery.queryName())
                .scale(currentRunningQuery.scale())
                .sparkVersion(spark.version())
                .executorInstances(config.spark().executorInstances())
                .executorCores(config.spark().executorCores())
                .executorMemoryMb(Utils.memoryStringToMb(config.spark().executorMemory()))
                .sparkConf(JavaConverters.mapAsJavaMapConverter(spark.conf().getAll()).asJava())
                .applicationId(spark.sparkContext().applicationId())
                .experimentStartTimestampMillis(startTime)
                .experimentEndTimestampMillis(endTime)
                .durationMillis(elapsed)
                .build()
                .toRow());
        currentRunningQuery = null;
    }

    public void abortBenchmark() {
        if (currentRunningQuery != null) {
            currentRunningQuery.timer().stop();
        }
        currentRunningQuery = null;
    }

    public void flushMetrics() {
        spark.createDataFrame(metrics, TpcdsBenchmarkMetric.SPARK_SCHEMA).write()
                .mode(SaveMode.Append)
                .format("json")
                .save(paths.metricsDir());
        metrics.clear();
    }

    @Value.Immutable
    @ImmutablesStyle
    interface RunningQuery {
        String queryName();

        int scale();

        Stopwatch timer();

        final class Builder extends ImmutableRunningQuery.Builder {}

        static Builder builder() {
            return new Builder();
        }
    }
}
