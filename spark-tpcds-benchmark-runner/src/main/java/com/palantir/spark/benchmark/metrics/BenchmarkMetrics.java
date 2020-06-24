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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Stopwatch;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.spark.benchmark.config.SparkConfiguration;
import com.palantir.spark.benchmark.immutables.ImmutablesStyle;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.immutables.value.Value;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import scala.collection.JavaConverters;

public final class BenchmarkMetrics {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
    private static final Path LOCAL_BUFFER_DIR = Paths.get("var", "data", "metrics-buffer");

    private final SparkConfiguration config;
    private final String resolvedExperimentName;
    private final BenchmarkPaths paths;
    private final SparkSession spark;
    private final DB mapDb;
    private final Map<RunningQuery, BenchmarkMetric> metricsBuffer;
    private final Map<RunningQuery, Stopwatch> timers = new HashMap<>();
    private Optional<RunningQuery> currentRunningQuery = Optional.empty();

    public BenchmarkMetrics(
            SparkConfiguration config, String resolvedExperimentName, BenchmarkPaths paths, SparkSession spark) {
        this.config = config;
        this.resolvedExperimentName = resolvedExperimentName;
        this.paths = paths;
        this.spark = spark;

        LOCAL_BUFFER_DIR.toFile().mkdirs();
        this.mapDb = DBMaker.fileDB(
                        LOCAL_BUFFER_DIR.resolve(UUID.randomUUID().toString()).toFile())
                .transactionEnable()
                .closeOnJvmShutdown()
                .make();
        this.metricsBuffer = mapDb.hashMap(
                        resolvedExperimentName,
                        JacksonSerializer.create(RunningQuery.class),
                        JacksonSerializer.create(BenchmarkMetric.class))
                .create();
    }

    public void startBenchmark(String queryName, int scale) {
        Preconditions.checkArgument(currentRunningQuery.isEmpty(), "Can only run one query at a time.");
        currentRunningQuery = Optional.of(
                RunningQuery.builder().queryName(queryName).scale(scale).build());
        timers.put(currentRunningQuery.get(), Stopwatch.createStarted());
    }

    public void stopBenchmark(String queryName, int scale) {
        Preconditions.checkArgument(currentRunningQuery.isPresent(), "No benchmark is currently running.");
        RunningQuery runningQuery = currentRunningQuery.get();
        Preconditions.checkArgument(
                runningQuery.queryName().equals(queryName) && runningQuery.scale() == scale,
                "Query names and scales must match",
                SafeArg.of("currentRunningQuery", runningQuery),
                SafeArg.of("queryName", queryName),
                SafeArg.of("scale", scale));

        Stopwatch stopped = Optional.ofNullable(timers.get(runningQuery))
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Missing timer!", SafeArg.of("queryName", queryName), SafeArg.of("scale", scale)));
        long endTime = System.currentTimeMillis();
        long elapsed = stopped.elapsed(TimeUnit.MILLISECONDS);
        long startTime = endTime - elapsed;

        metricsBuffer.put(
                runningQuery,
                BenchmarkMetric.builder()
                        .experimentName(resolvedExperimentName)
                        .queryName(runningQuery.queryName())
                        .scale(runningQuery.scale())
                        .sparkVersion(spark.version())
                        .executorInstances(config.executorInstances())
                        .executorCores(config.executorCores())
                        .executorMemoryMb(Utils.memoryStringToMb(config.executorMemory()))
                        .sparkConf(JavaConverters.mapAsJavaMapConverter(
                                        spark.conf().getAll())
                                .asJava())
                        .applicationId(spark.sparkContext().applicationId())
                        .experimentStartTimestampMillis(startTime)
                        .experimentEndTimestampMillis(endTime)
                        .durationMillis(elapsed)
                        .build());
        mapDb.commit();
        currentRunningQuery = Optional.empty();
    }

    public void abortBenchmark(String queryName, int scale) {
        currentRunningQuery.ifPresent(query -> {
            Preconditions.checkState(
                    query.queryName().equals(queryName) && query.scale() == scale,
                    "Query names and scales must match",
                    SafeArg.of("currentRunningQuery", query),
                    SafeArg.of("queryName", queryName),
                    SafeArg.of("scale", scale));
            Optional.ofNullable(timers.get(query)).map(Stopwatch::stop);
        });
        currentRunningQuery = Optional.empty();
    }

    public void markVerificationFailed(String queryName, int scale) {
        RunningQuery runningQuery =
                RunningQuery.builder().queryName(queryName).scale(scale).build();
        BenchmarkMetric metric = Optional.ofNullable(metricsBuffer.get(runningQuery))
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Cannot mark verification failure for non-existent result!",
                        SafeArg.of("query", runningQuery)));
        metricsBuffer.put(
                runningQuery,
                BenchmarkMetric.builder().from(metric).failedVerification(true).build());
        mapDb.commit();
    }

    public void flushMetrics() {
        getMetrics().write().mode(SaveMode.Append).format("json").save(paths.metricsDir());
        clearBuffer();
    }

    public Dataset<Row> getMetrics() {
        return spark.createDataFrame(
                metricsBuffer.values().stream().map(BenchmarkMetric::toRow).collect(Collectors.toList()),
                BenchmarkMetric.schema());
    }

    private void clearBuffer() {
        metricsBuffer.clear();
        mapDb.commit();
    }

    @Value.Immutable
    @ImmutablesStyle
    @JsonSerialize(as = ImmutableRunningQuery.class)
    @JsonDeserialize(as = ImmutableRunningQuery.class)
    interface RunningQuery {
        String queryName();

        int scale();

        final class Builder extends ImmutableRunningQuery.Builder {}

        static Builder builder() {
            return new Builder();
        }
    }

    private static class JacksonSerializer<T> implements Serializer<T> {
        private final Class<T> valueType;

        private JacksonSerializer(Class<T> valueType) {
            this.valueType = valueType;
        }

        public static <T> JacksonSerializer<T> create(Class<T> valueType) {
            return new JacksonSerializer<>(valueType);
        }

        public void serialize(DataOutput2 out, Object value) throws IOException {
            out.write(OBJECT_MAPPER.writeValueAsBytes(value));
        }

        public T deserialize(DataInput2 input, int available) throws IOException {
            return OBJECT_MAPPER.readValue(input, valueType);
        }
    }
}
