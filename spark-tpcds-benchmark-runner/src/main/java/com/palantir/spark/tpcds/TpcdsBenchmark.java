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

package com.palantir.spark.tpcds;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.tpcds.config.TpcdsBenchmarkConfig;
import com.palantir.spark.tpcds.correctness.TpcdsQueryCorrectnessChecks;
import com.palantir.spark.tpcds.datagen.TpcdsDataGenerator;
import com.palantir.spark.tpcds.metrics.TpcdsBenchmarkMetrics;
import com.palantir.spark.tpcds.paths.TpcdsPaths;
import com.palantir.spark.tpcds.queries.Query;
import com.palantir.spark.tpcds.queries.SortBenchmarkQuery;
import com.palantir.spark.tpcds.queries.SqlQuery;
import com.palantir.spark.tpcds.registration.TpcdsTableRegistration;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;
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

public final class TpcdsBenchmark {

    private static final Logger log = LoggerFactory.getLogger(TpcdsBenchmark.class);

    private static final ImmutableSet<String> BLACKLISTED_QUERIES =
            ImmutableSet.of("q23b.sql", "q39a.sql", "q39b.sql", "q14b.sql", "q49.sql", "q64.sql", "q77.sql");

    private final TpcdsBenchmarkConfig config;
    private final TpcdsDataGenerator dataGenerator;
    private final TpcdsTableRegistration registration;
    private final TpcdsPaths paths;
    private final TpcdsQueryCorrectnessChecks correctness;
    private final TpcdsBenchmarkMetrics metrics;
    private final SparkSession spark;
    private final FileSystem dataFileSystem;
    private final Supplier<ImmutableList<Query>> sqlQuerySupplier;

    public TpcdsBenchmark(
            TpcdsBenchmarkConfig config,
            TpcdsDataGenerator dataGenerator,
            TpcdsTableRegistration registration,
            TpcdsPaths paths,
            TpcdsQueryCorrectnessChecks correctness,
            TpcdsBenchmarkMetrics metrics,
            SparkSession spark,
            FileSystem dataFileSystem) {
        this.config = config;
        this.dataGenerator = dataGenerator;
        this.registration = registration;
        this.correctness = correctness;
        this.metrics = metrics;
        this.paths = paths;
        this.spark = spark;
        this.dataFileSystem = dataFileSystem;
        this.sqlQuerySupplier = Suppliers.memoize(() -> buildSqlQueries(spark));
    }

    public void run() throws IOException {
        dataGenerator.generateDataIfNecessary();
        for (int iteration = 0; iteration < config.iterations(); iteration++) {
            log.info(
                    "Beginning benchmark iteration {} of {}.",
                    SafeArg.of("currentIteration", iteration),
                    SafeArg.of("totalNumIterations", config.iterations()));
            config.dataScalesGb().forEach(scale -> {
                log.info("Beginning benchmarks at a new data scale of {}.", SafeArg.of("dataScale", scale));
                registration.registerTables(scale);
                getQueries().forEach(query -> {
                    log.info(
                            "Running query {}: {}",
                            SafeArg.of("queryName", query.getName()),
                            SafeArg.of("queryStatement", query.getSqlStatement().orElse("N/A")));
                    try {
                        String resultLocation = paths.experimentResultLocation(scale, query.getName());
                        Path resultPath = new Path(resultLocation);
                        if (dataFileSystem.exists(resultPath) && !dataFileSystem.delete(resultPath, true)) {
                            throw new IllegalStateException(String.format(
                                    "Failed to clear experiment result destination directory at %s.", resultPath));
                        }

                        spark.sparkContext().setJobDescription(String.format("%s-benchmark", query.getName()));
                        metrics.startBenchmark(query.getName(), scale);
                        boolean success = false;
                        try {
                            query.save(resultLocation);
                            success = true;
                        } finally {
                            if (success) {
                                metrics.stopBenchmark();
                            } else {
                                metrics.abortBenchmark();
                            }
                        }

                        log.info(
                                "Successfully ran query {} at scale {}.",
                                SafeArg.of("queryName", query.getName()),
                                SafeArg.of("scale", scale));
                        if (query.getSqlStatement().isPresent()) {
                            log.info(
                                    "Verifying correctness of query {} at scale {}.",
                                    SafeArg.of("queryName", query.getName()),
                                    SafeArg.of("scale", scale));
                            correctness.verifyCorrectness(
                                    scale,
                                    query.getName(),
                                    query.getSqlStatement().get(),
                                    query.getSchema(),
                                    resultLocation);
                            log.info(
                                    "Successfully verified correctness of query {} at scale {}.",
                                    SafeArg.of("queryName", query.getName()),
                                    SafeArg.of("scale", scale));
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                log.info("Successfully ran benchmarks at scale of {} GB.", SafeArg.of("scale", scale));
            });
            metrics.flushMetrics();
            log.info(
                    "Successfully finished an iteration of benchmarks at all scales. Completed {} iterations in total.",
                    SafeArg.of("completedIterations", iteration));
        }
        log.info("Successfully ran all benchmarks for the requested number of iterations");

        Dataset<Row> resultMetrics = spark.read().json(paths.metricsDir()).drop("sparkConf");
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

    private List<Query> getQueries() {
        ImmutableList.Builder<Query> queries = ImmutableList.builder();
        if (config.includeSortBenchmark()) {
            queries.add(new SortBenchmarkQuery(spark));
        }
        if (!config.excludeSqlQueries()) {
            queries.addAll(sqlQuerySupplier.get());
        }
        return queries.build();
    }

    private static ImmutableList<Query> buildSqlQueries(SparkSession spark) {
        ImmutableList.Builder<Query> queries = ImmutableList.builder();
        try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(
                        TpcdsBenchmark.class.getClassLoader().getResourceAsStream("queries.tar"));
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
