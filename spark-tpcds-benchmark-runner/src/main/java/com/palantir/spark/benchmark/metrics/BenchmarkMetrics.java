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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
    private static final Path LOCAL_BUFFER_DIR = Paths.get("var", "data", "metrics-buffer");

    private final SparkConfiguration config;
    private final String resolvedExperimentName;
    private final BenchmarkPaths paths;
    private final SparkSession spark;
    private final DB mapDb;
    private final Map<QuerySessionIdentifier, BenchmarkMetric> metricsBuffer;
    private Optional<RunningQuery> currentRunningQuery = Optional.empty();

    public BenchmarkMetrics(
            SparkConfiguration config, String resolvedExperimentName, BenchmarkPaths paths, SparkSession spark) {
        this.config = config;
        this.resolvedExperimentName = resolvedExperimentName;
        this.paths = paths;
        this.spark = spark;

        LOCAL_BUFFER_DIR.toFile().mkdirs();
        this.mapDb = DBMaker.fileDB(LOCAL_BUFFER_DIR.resolve("mapdb").toFile())
                .transactionEnable()
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .make();
        this.metricsBuffer = mapDb.hashMap(
                        resolvedExperimentName,
                        JacksonSerializer.create(QuerySessionIdentifier.class),
                        JacksonSerializer.create(BenchmarkMetric.class))
                .createOrOpen();
        if (!this.metricsBuffer.isEmpty()) {
            log.warn("Found unflushed metrics in the buffer; attempting to flush");
            flushMetrics();
        }
    }

    public void startBenchmark(String queryName, int scale) {
        Preconditions.checkArgument(currentRunningQuery.isEmpty(), "Can only run one query at a time.");
        currentRunningQuery = Optional.of(RunningQuery.builder()
                .queryName(queryName)
                .scale(scale)
                .timer(Stopwatch.createStarted())
                .build());
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

        Instant endTime = Instant.now();
        long elapsed = runningQuery.timer().elapsed(TimeUnit.MILLISECONDS);
        Instant startTime = endTime.minus(Duration.ofMillis(elapsed));

        writeToBuffer(
                runningQuery.toIdentifier(),
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
                        .experimentStartTimestampMillis(startTime.toEpochMilli())
                        .experimentEndTimestampMillis(endTime.toEpochMilli())
                        .durationMillis(elapsed)
                        .build());
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
            query.timer().stop();
        });
        currentRunningQuery = Optional.empty();
    }

    public void markVerificationFailed(String queryName, int scale) {
        QuerySessionIdentifier identifier = QuerySessionIdentifier.of(queryName, scale);
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
        mapDb.commit();
    }

    public void flushMetrics() {
        getMetricsDataset().write().mode(SaveMode.Append).format("json").save(paths.metricsDir());
        metricsBuffer.clear();
        mapDb.commit();
    }

    public Dataset<Row> getMetricsDataset() {
        return spark.createDataFrame(
                metricsBuffer.values().stream().map(BenchmarkMetric::toRow).collect(Collectors.toList()),
                BenchmarkMetric.schema());
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

        @Value.Lazy
        default QuerySessionIdentifier toIdentifier() {
            return QuerySessionIdentifier.builder()
                    .queryName(queryName())
                    .scale(scale())
                    .build();
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
