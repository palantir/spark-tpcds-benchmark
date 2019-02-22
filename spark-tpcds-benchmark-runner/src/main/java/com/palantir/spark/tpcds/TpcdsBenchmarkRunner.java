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
import com.palantir.spark.tpcds.config.TpcdsBenchmarkConfig;
import com.palantir.spark.tpcds.correctness.TpcdsQueryCorrectnessChecks;
import com.palantir.spark.tpcds.datagen.TpcdsDataGenerator;
import com.palantir.spark.tpcds.metrics.TpcdsBenchmarkMetrics;
import com.palantir.spark.tpcds.paths.TpcdsPaths;
import com.palantir.spark.tpcds.registration.TpcdsTableRegistration;
import com.palantir.spark.tpcds.schemas.TpcdsSchemas;
import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * The main entry point for the application.
 */
public final class TpcdsBenchmarkRunner {

    private static final Path DEFAULT_CONFIG_FILE = Paths.get("var", "conf", "config.yml");

    private TpcdsBenchmarkRunner() {}

    public static void main(String[] args) throws Exception {
        Path configFile;
        if (args.length == 0) {
            configFile = DEFAULT_CONFIG_FILE;
        } else {
            configFile = Paths.get(args[0]);
        }
        TpcdsBenchmarkConfig config = TpcdsBenchmarkConfig.parse(configFile);
        Configuration hadoopConf = new Configuration();
        for (Path hadoopConfDir : config.hadoop().hadoopConfDirs()) {
            hadoopConf = loadConfFromFile(hadoopConf, hadoopConfDir.toFile());
        }
        config.hadoop().hadoopConf().forEach(hadoopConf::set);
        try (FileSystem dataFileSystem = FileSystem.get(
                new org.apache.hadoop.fs.Path(config.testDataDir()).toUri(), hadoopConf)) {
            SparkConf sparkConf = new SparkConf().setMaster(config.spark().master());
            config.spark().sparkConf().forEach(sparkConf::set);
            hadoopConf.forEach(confEntry ->
                    sparkConf.set(
                            String.format("spark.hadoop.%s", confEntry.getKey()), confEntry.getValue()));
            // Force turn off dynamic allocation for consistent results
            if (!config.spark().master().startsWith("local")) {
                sparkConf.set("spark.dynamicAllocation.enabled", "false");
                sparkConf.set("spark.executor.instances", Integer.toString(config.spark().executorInstances()));
                sparkConf.set("spark.executor.cores", Integer.toString(config.spark().executorCores()));
                sparkConf.set("spark.executor.memory", config.spark().executorMemory());
            }

            SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
            TpcdsPaths paths = new TpcdsPaths(config.testDataDir());
            TpcdsSchemas schemas = new TpcdsSchemas();
            TpcdsTableRegistration registration = new TpcdsTableRegistration(
                    paths, dataFileSystem, spark, schemas);
            ExecutorService dataGeneratorThreadPool = Executors.newFixedThreadPool(
                    config.dataGenerationParallelism(),
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("data-generator-%d")
                            .build());
            TpcdsDataGenerator dataGenerator = new TpcdsDataGenerator(
                    config, dataFileSystem, spark, paths, schemas, dataGeneratorThreadPool);
            TpcdsQueryCorrectnessChecks correctness = new TpcdsQueryCorrectnessChecks(
                    paths, dataFileSystem, spark);
            TpcdsBenchmarkMetrics metrics = new TpcdsBenchmarkMetrics(config, paths, spark);
            new TpcdsBenchmark(
                    config,
                    dataGenerator,
                    registration,
                    paths,
                    correctness,
                    metrics,
                    spark,
                    dataFileSystem).run();
        }
    }

    private static Configuration loadConfFromFile(Configuration conf, File confFile) throws MalformedURLException {
        Configuration resolvedConfiguration = conf;
        if (confFile.isDirectory()) {
            for (File child : Optional.ofNullable(confFile.listFiles()).orElse(new File[0])) {
                resolvedConfiguration = loadConfFromFile(resolvedConfiguration, child);
            }
        } else if (confFile.isFile() && confFile.getName().endsWith(".xml")) {
            resolvedConfiguration.addResource(confFile.toURL());
        }
        return resolvedConfiguration;
    }
}
