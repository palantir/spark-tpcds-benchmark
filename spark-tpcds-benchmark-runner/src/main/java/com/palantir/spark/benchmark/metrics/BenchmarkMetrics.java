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

package com.palantir.spark.benchmark.metrics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.spark.benchmark.config.SparkConfiguration;
import com.palantir.spark.benchmark.immutables.ImmutablesStyle;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.util.Utils;
import org.immutables.value.Value;
import scala.collection.JavaConverters;

public final class BenchmarkMetrics {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
    private static final Path LOCAL_BUFFER_DIR = Paths.get("var", "data", "metrics-buffer");

    private final SparkConfiguration config;
    private final String resolvedExperimentName;
    private final List<Row> verifications = new ArrayList<>();
    private final BenchmarkPaths paths;
    private final SparkSession spark;
    private final File localBuffer;
    private RunningQuery currentRunningQuery;

    public BenchmarkMetrics(
            SparkConfiguration config, String resolvedExperimentName, BenchmarkPaths paths, SparkSession spark) {
        this.config = config;
        this.resolvedExperimentName = resolvedExperimentName;
        this.paths = paths;
        this.spark = spark;
        this.currentRunningQuery = null;

        LOCAL_BUFFER_DIR.toFile().mkdirs();
        this.localBuffer =
                LOCAL_BUFFER_DIR.resolve(UUID.randomUUID().toString()).toFile();
        clearBuffer();
    }

    public void startBenchmark(String queryName, int scale) {
        Preconditions.checkArgument(currentRunningQuery == null, "Can only run one query at a time.");
        currentRunningQuery = RunningQuery.builder()
                .queryName(queryName)
                .scale(scale)
                .timer(Stopwatch.createStarted())
                .build();
    }

    public void stopBenchmark(String queryName, int scale) {
        Preconditions.checkArgument(currentRunningQuery != null, "No benchmark is currently running.");
        Preconditions.checkArgument(
                currentRunningQuery.queryName().equals(queryName) && currentRunningQuery.scale() == scale,
                "Query names and scales must match",
                SafeArg.of("currentRunningQuery", currentRunningQuery),
                SafeArg.of("queryName", queryName),
                SafeArg.of("scale", scale));
        Stopwatch stopped = currentRunningQuery.timer();
        long endTime = System.currentTimeMillis();
        long elapsed = stopped.elapsed(TimeUnit.MILLISECONDS);
        long startTime = endTime - elapsed;

        BenchmarkMetric currentMetric = BenchmarkMetric.builder()
                .experimentName(resolvedExperimentName)
                .queryName(currentRunningQuery.queryName())
                .scale(currentRunningQuery.scale())
                .sparkVersion(spark.version())
                .executorInstances(config.executorInstances())
                .executorCores(config.executorCores())
                .executorMemoryMb(Utils.memoryStringToMb(config.executorMemory()))
                .sparkConf(JavaConverters.mapAsJavaMapConverter(spark.conf().getAll())
                        .asJava())
                .applicationId(spark.sparkContext().applicationId())
                .experimentStartTimestampMillis(startTime)
                .experimentEndTimestampMillis(endTime)
                .durationMillis(elapsed)
                .build();

        List<BenchmarkMetric> newMetrics = ImmutableList.<BenchmarkMetric>builder()
                .addAll(readFromBuffer())
                .add(currentMetric)
                .build();
        writeToBuffer(newMetrics);
        currentRunningQuery = null;
    }

    public void abortBenchmark(String queryName, int scale) {
        if (currentRunningQuery != null) {
            Preconditions.checkState(
                    currentRunningQuery.queryName().equals(queryName) && currentRunningQuery.scale() == scale,
                    "Query names and scales must match",
                    SafeArg.of("currentRunningQuery", currentRunningQuery),
                    SafeArg.of("queryName", queryName),
                    SafeArg.of("scale", scale));
            currentRunningQuery.timer().stop();
        }
        currentRunningQuery = null;
    }

    public void markVerificationFailed(String queryName, int scale) {
        verifications.add(VerificationStatus.builder()
                .experimentName(resolvedExperimentName)
                .queryName(queryName)
                .scale(scale)
                .failedVerification(true)
                .build()
                .toRow());
    }

    public void flushMetrics() {
        getMetrics().write().mode(SaveMode.Append).format("json").save(paths.metricsDir());
        clearBuffer();
    }

    public Dataset<Row> getMetrics() {
        Dataset<Row> metricsDf = spark.createDataFrame(
                readFromBuffer().stream().map(BenchmarkMetric::toRow).collect(Collectors.toList()),
                BenchmarkMetric.schema());
        Dataset<Row> withVerifications = metricsDf.join(
                spark.createDataFrame(verifications, VerificationStatus.schema()),
                JavaConverters.asScalaBuffer(ImmutableList.of("experimentName", "queryName", "scale")),
                "left");
        // coalesce verification statuses (default value is failedVerification = false)
        return withVerifications
                .withColumn(
                        "failedVerification",
                        functions.coalesce(
                                withVerifications.col("failedVerification"),
                                withVerifications.col(VerificationStatus.FAILED_VERIFICATION_COL_NAME),
                                functions.lit(false)))
                .drop(VerificationStatus.FAILED_VERIFICATION_COL_NAME);
    }

    private List<BenchmarkMetric> readFromBuffer() {
        try {
            return OBJECT_MAPPER.readValue(localBuffer, new TypeReference<>() {});
        } catch (IOException e) {
            throw new SafeRuntimeException("Exception while reading metrics from local buffer", e);
        }
    }

    private void writeToBuffer(List<BenchmarkMetric> metrics) {
        try {
            OBJECT_MAPPER.writeValue(localBuffer, metrics);
        } catch (IOException e) {
            throw new SafeRuntimeException("Exception while writing metrics to local buffer", e);
        }
    }

    private void clearBuffer() {
        writeToBuffer(ImmutableList.of());
    }

    @Value.Immutable
    @ImmutablesStyle
    interface RunningQuery {
        String queryName();

        int scale();

        @Value.Auxiliary
        Stopwatch timer();

        final class Builder extends ImmutableRunningQuery.Builder {}

        static Builder builder() {
            return new Builder();
        }
    }
}
