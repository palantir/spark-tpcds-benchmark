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

package com.palantir.spark.benchmark;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.io.CharStreams;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.benchmark.config.BenchmarkRunnerConfig;
import com.palantir.spark.benchmark.correctness.TpcdsQueryCorrectnessChecks;
import com.palantir.spark.benchmark.datagen.SortDataGenerator;
import com.palantir.spark.benchmark.datagen.TpcdsDataGenerator;
import com.palantir.spark.benchmark.metrics.BenchmarkMetric;
import com.palantir.spark.benchmark.metrics.BenchmarkMetrics;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import com.palantir.spark.benchmark.queries.Query;
import com.palantir.spark.benchmark.queries.QuerySessionIdentifier;
import com.palantir.spark.benchmark.queries.SortBenchmarkQuery;
import com.palantir.spark.benchmark.queries.SqlQuery;
import com.palantir.spark.benchmark.registration.TableRegistration;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Benchmark {

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);
    private static final ImmutableSet<String> BLACKLISTED_QUERIES =
            ImmutableSet.of("q23b.sql", "q39a.sql", "q39b.sql", "q14b.sql", "q49.sql", "q64.sql", "q77.sql");

    private final BenchmarkRunnerConfig config;
    private final TpcdsDataGenerator dataGenerator;
    private final SortDataGenerator sortDataGenerator;
    private final TableRegistration registration;
    private final BenchmarkPaths paths;
    private final TpcdsQueryCorrectnessChecks correctness;
    private final BenchmarkMetrics metrics;
    private final SparkSession spark;
    private final FileSystem dataFileSystem;
    private final Supplier<ImmutableList<Query>> sqlQuerySupplier;
    private final Map<QuerySessionIdentifier, Integer> attemptCounters = new ConcurrentHashMap<>();

    public Benchmark(
            BenchmarkRunnerConfig config,
            TpcdsDataGenerator dataGenerator,
            SortDataGenerator sortDataGenerator,
            TableRegistration registration,
            BenchmarkPaths paths,
            TpcdsQueryCorrectnessChecks correctness,
            BenchmarkMetrics metrics,
            SparkSession spark,
            FileSystem dataFileSystem) {
        this.config = config;
        this.dataGenerator = dataGenerator;
        this.sortDataGenerator = sortDataGenerator;
        this.registration = registration;
        this.correctness = correctness;
        this.metrics = metrics;
        this.paths = paths;
        this.spark = spark;
        this.dataFileSystem = dataFileSystem;
        this.sqlQuerySupplier = Suppliers.memoize(() -> buildSqlQueries(spark));
    }

    public void run() throws IOException {
        if (config.dataGeneration().tpcds().enabled()) {
            dataGenerator.generateData();
        }
        if (config.dataGeneration().gensort().enabled()) {
            sortDataGenerator.generate();
        }
        for (int iteration = 1; iteration <= config.benchmarks().iterations(); iteration++) {
            log.info(
                    "Beginning benchmark iteration {} of {}.",
                    SafeArg.of("currentIteration", iteration),
                    SafeArg.of("totalNumIterations", config.benchmarks().iterations()));
            Streams.forEachPair(
                    config.dataScalesGb().stream(), Stream.generate(Suppliers.ofInstance(iteration)), (scale, iter) -> {
                        log.info("Beginning benchmarks at a new data scale of {}.", SafeArg.of("dataScale", scale));
                        registration.registerTpcdsTables(scale);
                        registration.registerGensortTable(scale);
                        getQueries()
                                .forEach(query -> runQueryWithRetries(
                                        query, scale, iter, config.benchmarks().attemptsPerQuery()));
                        log.info("Successfully ran benchmarks at scale of {} GB.", SafeArg.of("scale", scale));
                    });
            log.info(
                    "Successfully finished an iteration of benchmarks at all scales. Completed {} iterations in total.",
                    SafeArg.of("completedIterations", iteration));
        }
        log.info("Successfully ran all benchmarks for the requested number of iterations");

        metrics.flushMetrics();
        Dataset<Row> resultMetrics = spark.read()
                .schema(BenchmarkMetric.schema())
                .json(paths.metricsDir())
                .drop("sparkConf");
        log.info(
                "Printing summary metrics (limit 1000):\n{}",
                SafeArg.of(
                        "metrics",
                        resultMetrics
                                .groupBy("queryName", "scale")
                                .agg(functions.avg("durationMillis"), functions.max("durationMillis"))
                                .showString(1000, 20, false)));
        log.info("Finished benchmark; exiting");
    }

    private void runQueryWithRetries(Query query, int scale, int iteration, int numAttempts) {
        try {
            RetryerBuilder.<Boolean>newBuilder()
                    .retryIfException()
                    .retryIfRuntimeException()
                    .retryIfResult(succeeded -> succeeded == null || !succeeded)
                    .withStopStrategy(StopStrategies.stopAfterAttempt(numAttempts))
                    .build()
                    .call(() -> attemptQuery(query, scale, iteration));
        } catch (RetryException | ExecutionException e) {
            log.error(
                    "Failed to execute query {} at scale {} on all {} attempts in iteration {}",
                    SafeArg.of("query", query.getName()),
                    SafeArg.of("scale", scale),
                    SafeArg.of("numAttempts", numAttempts),
                    SafeArg.of("iteration", iteration),
                    e);
        }
    }

    private boolean attemptQuery(Query query, int scale, int iteration) {
        QuerySessionIdentifier identifier = QuerySessionIdentifier.createDefault(query.getName(), scale, iteration);
        int attempt =
                attemptCounters.compute(identifier, (_key, oldAttempts) -> oldAttempts == null ? 0 : oldAttempts + 1);
        log.info(
                "Running query {} (attempt {}): {}",
                SafeArg.of("identifier", identifier),
                SafeArg.of("attempt", attempt),
                SafeArg.of("queryStatement", query.getSqlStatement().orElse("N/A")));
        try {
            String resultLocation = paths.experimentResultLocation(identifier, attempt);
            Path resultPath = new Path(resultLocation);
            if (dataFileSystem.exists(resultPath) && !dataFileSystem.delete(resultPath, true)) {
                throw new IllegalStateException(
                        String.format("Failed to clear experiment result destination directory at %s.", resultPath));
            }

            spark.sparkContext().setJobDescription(String.format("%s-benchmark-attempt-%d", query.getName(), attempt));
            metrics.startBenchmark(identifier, attempt);
            query.save(resultLocation);
            metrics.stopBenchmark(identifier, attempt);
            log.info(
                    "Successfully ran query {} at scale {} on attempt {}.",
                    SafeArg.of("queryName", query.getName()),
                    SafeArg.of("scale", scale),
                    SafeArg.of("attempt", attempt));
            verifyCorrectness(query, identifier, resultLocation);
            return true;
        } catch (Exception e) {
            metrics.abortBenchmark(identifier, attempt);
            log.error(
                    "Caught an exception while running query {} at scale {} on attempt {}; may re-attempt.",
                    SafeArg.of("queryName", query.getName()),
                    SafeArg.of("scale", scale),
                    SafeArg.of("attempt", attempt),
                    e);
            return false;
        }
    }

    private void verifyCorrectness(Query query, QuerySessionIdentifier identifier, String resultLocation) {
        if (query.getSqlStatement().isPresent()) {
            log.info(
                    "Verifying correctness of query {} at scale {}.",
                    SafeArg.of("queryName", query.getName()),
                    SafeArg.of("scale", identifier.scale()));
            try {
                correctness.verifyCorrectness(
                        identifier.scale(),
                        query.getName(),
                        query.getSqlStatement().get(),
                        query.getSchema(),
                        resultLocation);
                log.info(
                        "Successfully verified correctness of query {} at scale {}.",
                        SafeArg.of("queryName", query.getName()),
                        SafeArg.of("scale", identifier.scale()));
            } catch (IOException | RuntimeException e) {
                log.info(
                        "Failed to verify correctness of query {} at scale {}.",
                        SafeArg.of("queryName", query.getName()),
                        SafeArg.of("scale", identifier.scale()),
                        e);
                metrics.markVerificationFailed(identifier);
            }
        }
    }

    private List<Query> getQueries() {
        ImmutableList.Builder<Query> queries = ImmutableList.builder();
        if (config.benchmarks().gensort().enabled()) {
            queries.add(new SortBenchmarkQuery(spark));
        }
        if (config.benchmarks().tpcds().enabled()) {
            queries.addAll(sqlQuerySupplier.get());
        }
        return queries.build();
    }

    private static ImmutableList<Query> buildSqlQueries(SparkSession spark) {
        ImmutableList.Builder<Query> queries = ImmutableList.builder();
        try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(
                        Benchmark.class.getClassLoader().getResourceAsStream("queries.tar"));
                InputStreamReader tarArchiveReader =
                        new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8)) {
            TarArchiveEntry entry;
            while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
                String queryString = CharStreams.toString(tarArchiveReader);
                if (!BLACKLISTED_QUERIES.contains(entry.getName())) {
                    queries.add(new SqlQuery(spark, entry.getName(), queryString));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return queries.build();
    }
}
