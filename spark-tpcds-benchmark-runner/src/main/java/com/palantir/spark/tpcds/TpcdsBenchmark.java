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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.tpcds.config.TpcdsBenchmarkConfig;
import com.palantir.spark.tpcds.correctness.TpcdsQueryCorrectnessChecks;
import com.palantir.spark.tpcds.datagen.TpcdsDataGenerator;
import com.palantir.spark.tpcds.metrics.TpcdsBenchmarkMetrics;
import com.palantir.spark.tpcds.paths.TpcdsPaths;
import com.palantir.spark.tpcds.registration.TpcdsTableRegistration;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TpcdsBenchmark {

    private static final Logger log = LoggerFactory.getLogger(TpcdsBenchmark.class);

    private static final ImmutableSet<String> BLACKLISTED_QUERIES = ImmutableSet.of(
            "q23b.sql",
            "q39a.sql",
            "q39b.sql",
            "q14b.sql",
            "q49.sql",
            "q64.sql",
            "q77.sql");

    private final TpcdsBenchmarkConfig config;
    private final TpcdsDataGenerator dataGenerator;
    private final TpcdsTableRegistration registration;
    private final TpcdsPaths paths;
    private final TpcdsQueryCorrectnessChecks correctness;
    private final TpcdsBenchmarkMetrics metrics;
    private final SparkSession spark;
    private final FileSystem dataFileSystem;
    private final Supplier<Map<String, String>> queries = Suppliers.memoize(TpcdsBenchmark::getQueries);

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
    }

    public void run() throws IOException {
        dataGenerator.generateDataIfNecessary();
        for (int iteration = 0; iteration < config.iterations(); iteration++) {
            log.info("Beginning benchmark iteration.",
                    SafeArg.of("currentIteration", iteration),
                    SafeArg.of("totalNumIterations", config.iterations()));
            config.dataScalesGb().forEach(scale -> {
                log.info("Beginning benchmarks at a new data scale.",
                        SafeArg.of("dataScale", scale));
                registration.registerTables(scale);
                queries.get().forEach((queryName, query) -> {
                    log.info("Running query.",
                            SafeArg.of("queryName", queryName),
                            SafeArg.of("queryStatement", query));
                    try {
                        String resultLocation = paths.experimentResultLocation(scale, queryName);
                        Path resultPath = new Path(resultLocation);
                        if (dataFileSystem.exists(resultPath) && !dataFileSystem.delete(resultPath, true)) {
                            throw new IllegalStateException(
                                    String.format(
                                            "Failed to clear experiment result destination directory at %s.",
                                            resultPath));
                        }
                        Dataset<Row> queryResultDataset = sanitizeColumnNames(spark.sql(query));
                        spark.sparkContext().setJobDescription(String.format("%s-benchmark", queryName));
                        metrics.startBenchmark(queryName, scale);
                        boolean success = false;
                        try {
                            queryResultDataset.write().format("parquet").save(resultLocation);
                            success = true;
                        } finally {
                            if (success) {
                                metrics.stopBenchmark();
                            } else {
                                metrics.abortBenchmark();
                            }
                        }
                        log.info("Successfully ran query. Will now proceed to verify the correctness.",
                                SafeArg.of("queryName", queryName),
                                SafeArg.of("scale", scale));
                        correctness.verifyCorrectness(
                                scale, queryName, query, queryResultDataset.schema(), resultLocation);
                        log.info("Successfully verified correctness of a query.",
                                SafeArg.of("queryName", queryName),
                                SafeArg.of("scale", scale));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                log.info("Successfully ran benchmarks at a given scale.",
                        SafeArg.of("scale", scale));
            });
            metrics.flushMetrics();
            log.info("Successfully finished one iteration of benchmarks at all scales.",
                    SafeArg.of("completedIterations", iteration));
        }
        log.info("Successfully ran all benchmarks at the requested number of iterations. Exiting.");
    }

    private static Map<String, String> getQueries() {
        ImmutableMap.Builder<String, String> queries = ImmutableMap.builder();
        try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(
                TpcdsBenchmark.class.getClassLoader().getResourceAsStream("queries.tar"));
                InputStreamReader tarArchiveReader = new InputStreamReader(
                        tarArchiveInputStream, StandardCharsets.UTF_8)) {
            TarArchiveEntry entry;
            while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
                String queryString = CharStreams.toString(tarArchiveReader);
                if (!BLACKLISTED_QUERIES.contains(entry.getName())) {
                    queries.put(entry.getName(), queryString);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return queries.build();
    }

    private Dataset<Row> sanitizeColumnNames(Dataset<Row> sqlOutput) {
        Dataset<Row> sanitizedSqlOutput = sqlOutput;
        for (String columnName : sqlOutput.columns()) {
            String sanitized = sanitize(columnName);
            if (!sanitized.equals(columnName)) {
                sanitizedSqlOutput = sanitizedSqlOutput.withColumnRenamed(columnName, sanitized);
            }
        }
        return sanitizedSqlOutput;
    }

    private static String sanitize(String name) {
        return name.replaceAll("[ ,;{}()]", "X");
    }
}
