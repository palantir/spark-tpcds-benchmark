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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Stopwatch;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.spark.benchmark.config.SparkConfiguration;
import com.palantir.spark.benchmark.immutables.ImmutablesStyle;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import com.palantir.spark.benchmark.queries.QuerySessionIdentifier;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

public final class BenchmarkMetrics {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkMetrics.class);
    private static final DB MAP_DB = initializeMapDb();

    private final SparkConfiguration config;
    private final String resolvedExperimentName;
    private final BenchmarkPaths paths;
    private final SparkSession spark;
    private final Map<QuerySessionIdentifier, BenchmarkMetric> metricsBuffer;
    private Optional<RunningQuery> currentRunningQuery = Optional.empty();

    public BenchmarkMetrics(
            SparkConfiguration config, String resolvedExperimentName, BenchmarkPaths paths, SparkSession spark) {
        this.config = config;
        this.resolvedExperimentName = resolvedExperimentName;
        this.paths = paths;
        this.spark = spark;
        this.metricsBuffer = MAP_DB.hashMap(
                        resolvedExperimentName,
                        JacksonSerializer.create(QuerySessionIdentifier.class),
                        JacksonSerializer.create(BenchmarkMetric.class))
                .createOrOpen();
        if (!this.metricsBuffer.isEmpty()) {
            log.warn("Found unflushed metrics in the buffer; attempting to flush");
            flushMetrics();
        }
    }

    private static DB initializeMapDb() {
        Path localBufferDir = Paths.get("var", "data", "metrics-buffer");
        localBufferDir.toFile().mkdirs();
        return DBMaker.fileDB(localBufferDir.resolve("mapdb").toFile())
                .transactionEnable()
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .make();
    }

    public void startBenchmark(QuerySessionIdentifier identifier) {
        Preconditions.checkArgument(currentRunningQuery.isEmpty(), "Can only run one query at a time.");
        currentRunningQuery = Optional.of(RunningQuery.builder()
                .timer(Stopwatch.createStarted())
                .identifier(identifier)
                .build());
    }

    public void stopBenchmark(QuerySessionIdentifier identifier) {
        Preconditions.checkArgument(currentRunningQuery.isPresent(), "No benchmark is currently running.");
        RunningQuery runningQuery = currentRunningQuery.get();
        Preconditions.checkState(
                runningQuery.identifier().equals(identifier),
                "Query identifiers must match",
                SafeArg.of("currentlyRunningIdentifier", runningQuery.identifier()),
                SafeArg.of("identifier", identifier));

        Instant endTime = Instant.now();
        long elapsed = runningQuery.timer().elapsed(TimeUnit.MILLISECONDS);
        Instant startTime = endTime.minus(Duration.ofMillis(elapsed));

        writeToBuffer(
                runningQuery.identifier(),
                BenchmarkMetric.builder()
                        .experimentName(resolvedExperimentName)
                        .queryName(runningQuery.identifier().queryName())
                        .scale(runningQuery.identifier().scale())
                        .sparkVersion(spark.version())
                        .executorInstances(config.executorInstances())
                        .executorCores(config.executorCores())
                        .executorMemoryMb(Utils.memoryStringToMb(config.executorMemory()))
                        .sparkConf(JavaConverters.mapAsJavaMapConverter(
                                        spark.conf().getAll())
                                .asJava())
                        .applicationId(spark.sparkContext().applicationId())
                        .experimentStartTimestampMillis(startTime.toEpochMilli())
                        .experimentEndTimestampMillis(endTime.toEpochMilli())
                        .durationMillis(elapsed)
                        .sessionId(runningQuery.identifier().session())
                        .build());
        currentRunningQuery = Optional.empty();
    }

    public void abortBenchmark(QuerySessionIdentifier identifier) {
        currentRunningQuery.ifPresent(query -> {
            Preconditions.checkState(
                    query.identifier().equals(identifier),
                    "Query identifiers must match",
                    SafeArg.of("currentlyRunningIdentifier", query.identifier()),
                    SafeArg.of("identifier", identifier));
            query.timer().stop();
        });
        currentRunningQuery = Optional.empty();
    }

    public void markVerificationFailed(QuerySessionIdentifier identifier) {
        BenchmarkMetric metric = Optional.ofNullable(metricsBuffer.get(identifier))
                .orElseThrow(() -> new SafeIllegalArgumentException(
                        "Cannot mark verification failure for non-existent result!",
                        SafeArg.of("identifier", identifier)));
        writeToBuffer(
                identifier,
                BenchmarkMetric.builder().from(metric).failedVerification(true).build());
    }

    public void writeToBuffer(QuerySessionIdentifier identifier, BenchmarkMetric metric) {
        metricsBuffer.put(identifier, metric);
        MAP_DB.commit();
    }

    public void flushMetrics() {
        getMetricsDataset()
                .write()
                .partitionBy("sessionId")
                .mode(SaveMode.Append)
                .format("json")
                .save(paths.metricsDir());
        metricsBuffer.clear();
        MAP_DB.commit();
    }

    public Dataset<Row> getMetricsDataset() {
        return spark.createDataFrame(
                metricsBuffer.values().stream().map(BenchmarkMetric::toRow).collect(Collectors.toList()),
                BenchmarkMetric.schema());
    }

    @Value.Immutable
    @ImmutablesStyle
    interface RunningQuery {
        QuerySessionIdentifier identifier();

        @Value.Auxiliary
        Stopwatch timer();

        final class Builder extends ImmutableRunningQuery.Builder {}

        static Builder builder() {
            return new Builder();
        }
    }

    private static final class JacksonSerializer<T> implements Serializer<T> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
        private final Class<T> valueType;

        private JacksonSerializer(Class<T> valueType) {
            this.valueType = valueType;
        }

        public static <T> JacksonSerializer<T> create(Class<T> valueType) {
            return new JacksonSerializer<>(valueType);
        }

        @Override
        public void serialize(DataOutput2 out, Object value) throws IOException {
            out.write(OBJECT_MAPPER.writeValueAsBytes(value));
        }

        @Override
        public T deserialize(DataInput2 input, int _available) throws IOException {
            return OBJECT_MAPPER.readValue(input, valueType);
        }
    }
}
