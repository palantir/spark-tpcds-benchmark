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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.spark.benchmark.config.BenchmarkRunnerConfig;
import com.palantir.spark.benchmark.correctness.TpcdsQueryCorrectnessChecks;
import com.palantir.spark.benchmark.datagen.DefaultParquetTransformer;
import com.palantir.spark.benchmark.datagen.GenSortDataGenerator;
import com.palantir.spark.benchmark.datagen.SortDataGenerator;
import com.palantir.spark.benchmark.datagen.TpcdsDataGenerator;
import com.palantir.spark.benchmark.metrics.BenchmarkMetrics;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import com.palantir.spark.benchmark.registration.TableRegistration;
import com.palantir.spark.benchmark.schemas.Schemas;
import com.palantir.spark.benchmark.util.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The main entry point for the application. */
public final class BenchmarkRunner {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    private static final Path DEFAULT_CONFIG_FILE = Paths.get("var", "conf", "config.yml");

    private BenchmarkRunner() {}

    public static void main(String[] args) throws Exception {
        Path configFile;
        if (args.length == 0) {
            configFile = DEFAULT_CONFIG_FILE;
        } else {
            configFile = Paths.get(args[0]);
        }
        BenchmarkRunnerConfig config = BenchmarkRunnerConfig.parse(configFile);
        Configuration hadoopConf = config.hadoop().toHadoopConf();
        try (FileSystem dataFileSystem =
                FileSystems.createFileSystem(config.hadoop().defaultFsBaseUri(), hadoopConf)) {
            SparkConf sparkConf = new SparkConf().setMaster(config.spark().master());
            config.spark().sparkConf().forEach(sparkConf::set);
            hadoopConf.forEach(confEntry ->
                    sparkConf.set(String.format("spark.hadoop.%s", confEntry.getKey()), confEntry.getValue()));

            // Force turn off dynamic allocation for consistent results
            if (!config.spark().master().startsWith("local")) {
                sparkConf.set("spark.dynamicAllocation.enabled", "false");
                sparkConf.set(
                        "spark.executor.instances",
                        Integer.toString(config.spark().executorInstances()));
                sparkConf.set(
                        "spark.executor.cores", Integer.toString(config.spark().executorCores()));
                sparkConf.set("spark.executor.memory", config.spark().executorMemory());
            }

            String experimentName = config.benchmarks().experimentName().orElseGet(() -> Instant.now()
                    .toString());
            BenchmarkPaths paths = new BenchmarkPaths(experimentName);
            Schemas schemas = new Schemas();
            ExecutorService dataGeneratorThreadPool = Executors.newFixedThreadPool(
                    config.dataGeneration().parallelism(),
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("data-generator-%d")
                            .build());
            DefaultParquetTransformer parquetTransformer = new DefaultParquetTransformer();

            Supplier<SparkSession> spark = () -> {
                SparkSession sparkSession =
                        SparkSession.builder().config(sparkConf).getOrCreate();
                TableRegistration registration = new TableRegistration(paths, dataFileSystem, sparkSession, schemas);
                try {
                    config.dataScalesGb().forEach(scale -> {
                        registration.registerGensortTable(scale);
                        registration.registerTpcdsTables(scale);
                    });
                } catch (Exception e) {
                    log.warn("Table registration failed, will try to continue anyway.", e);
                }
                return sparkSession;
            };
            TpcdsDataGenerator dataGenerator = new TpcdsDataGenerator(
                    Paths.get(config.dataGeneration().tempWorkingDir()),
                    config.dataScalesGb(),
                    config.dataGeneration().overwriteData(),
                    dataFileSystem,
                    parquetTransformer,
                    spark.get(),
                    paths,
                    schemas,
                    dataGeneratorThreadPool);
            SortDataGenerator sortDataGenerator = new GenSortDataGenerator(
                    spark.get(),
                    dataFileSystem,
                    parquetTransformer,
                    paths,
                    schemas,
                    Paths.get(config.dataGeneration().tempWorkingDir()),
                    config.dataScalesGb(),
                    config.dataGeneration().overwriteData(),
                    dataGeneratorThreadPool);
            TpcdsQueryCorrectnessChecks correctness = new TpcdsQueryCorrectnessChecks(paths, dataFileSystem, spark);
            BenchmarkMetrics metrics = new BenchmarkMetrics(config.spark(), experimentName, paths, spark);
            new Benchmark(config, dataGenerator, sortDataGenerator, paths, correctness, metrics, spark, dataFileSystem)
                    .run();
        }
    }
}
