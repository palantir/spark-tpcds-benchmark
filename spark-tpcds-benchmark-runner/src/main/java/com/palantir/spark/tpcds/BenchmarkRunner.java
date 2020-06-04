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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.spark.tpcds.config.BenchmarkConfig;
import com.palantir.spark.tpcds.correctness.TpcdsQueryCorrectnessChecks;
import com.palantir.spark.tpcds.datagen.DefaultParquetTransformer;
import com.palantir.spark.tpcds.datagen.TpcdsDataGenerator;
import com.palantir.spark.tpcds.metrics.BenchmarkMetrics;
import com.palantir.spark.tpcds.paths.BenchmarkPaths;
import com.palantir.spark.tpcds.registration.TableRegistration;
import com.palantir.spark.tpcds.schemas.Schemas;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/** The main entry point for the application. */
public final class BenchmarkRunner {

    private static final Path DEFAULT_CONFIG_FILE = Paths.get("var", "conf", "config.yml");

    private BenchmarkRunner() {}

    public static void main(String[] args) throws Exception {
        Path configFile;
        if (args.length == 0) {
            configFile = DEFAULT_CONFIG_FILE;
        } else {
            configFile = Paths.get(args[0]);
        }
        BenchmarkConfig config = BenchmarkConfig.parse(configFile);
        Configuration hadoopConf = config.hadoop().toHadoopConf();
        try (FileSystem dataFileSystem =
                FileSystem.get(new org.apache.hadoop.fs.Path(config.testDataDir()).toUri(), hadoopConf)) {
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

            SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
            BenchmarkPaths paths = new BenchmarkPaths(config.testDataDir());
            Schemas schemas = new Schemas();
            TableRegistration registration = new TableRegistration(paths, dataFileSystem, spark, schemas);
            ExecutorService dataGeneratorThreadPool = Executors.newFixedThreadPool(
                    config.dataGenerationParallelism(),
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("data-generator-%d")
                            .build());
            TpcdsDataGenerator dataGenerator = new TpcdsDataGenerator(
                    config.dsdgenWorkLocalDir(),
                    config.dataScalesGb(),
                    config.overwriteData(),
                    dataFileSystem,
                    new DefaultParquetTransformer(),
                    spark,
                    paths,
                    schemas,
                    dataGeneratorThreadPool);
            TpcdsQueryCorrectnessChecks correctness = new TpcdsQueryCorrectnessChecks(paths, dataFileSystem, spark);
            BenchmarkMetrics metrics = new BenchmarkMetrics(config, paths, spark);
            new Benchmark(config, dataGenerator, registration, paths, correctness, metrics, spark, dataFileSystem)
                    .run();
        }
    }
}
