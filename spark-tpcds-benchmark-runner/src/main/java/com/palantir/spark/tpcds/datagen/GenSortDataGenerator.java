/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.spark.tpcds.datagen;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.spark.tpcds.paths.BenchmarkPaths;
import com.palantir.spark.tpcds.registration.TableRegistration;
import com.palantir.spark.tpcds.util.DataGenUtils;
import com.palantir.spark.tpcds.util.MoreFutures;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GenSortDataGenerator implements SortDataGenerator {
    private static final Logger log = LoggerFactory.getLogger(GenSortDataGenerator.class);

    private static final int BYTES_PER_RECORD = 100;
    private static final long BYTES_PER_GB = 1024 * 1024 * 1024;
    private static final long PARTITION_SIZE = BYTES_PER_GB; // 1GB
    private static final long RECORDS_PER_PARTITION = PARTITION_SIZE / BYTES_PER_RECORD;

    private static final Path GEN_SORT_MACOS_PATH = Paths.get("service", "bin", "gensort", "gensort_osx");
    private static final Path GENSORT_TGZ_LINUX_PATH =
            Paths.get("service", "bin", "gensort", "gensort-linux-1.5.tar.gz");
    private static final String GENSORT_LINUX_BINARY_FILE_NAME = "gensort";

    private static final String GENSORT_BIN_DIR_NAME = "bin";
    private static final String GENSORT_DATA_DIR_NAME = "data";
    private static final String GENERATED_DATA_FILE_NAME = "gensort_data";

    private final SparkSession spark;
    private final FileSystem destinationFileSystem;
    private final ParquetTransformer parquetTransformer;
    private final BenchmarkPaths paths;
    private final TableRegistration registration;
    private final Path tempWorkingDir;
    private final List<ScaleAndRecords> scaleAndRecords;
    private final ListeningExecutorService dataGeneratorThreadPool;

    public GenSortDataGenerator(
            SparkSession spark,
            FileSystem destinationFileSystem,
            ParquetTransformer parquetTransformer,
            BenchmarkPaths paths,
            TableRegistration registration,
            Path tempWorkingDir,
            List<Integer> scales,
            ExecutorService dataGeneratorThreadPool) {
        this.spark = spark;
        this.destinationFileSystem = destinationFileSystem;
        this.parquetTransformer = parquetTransformer;
        this.paths = paths;
        this.registration = registration;
        this.tempWorkingDir = tempWorkingDir;
        this.scaleAndRecords = scales.stream()
                .map(scale -> ScaleAndRecords.builder()
                        .scale(scale)
                        .numRecords(estimateNumRecords(scale))
                        .build())
                .collect(Collectors.toList());
        this.dataGeneratorThreadPool = MoreExecutors.listeningDecorator(dataGeneratorThreadPool);
    }

    // We'd like to generate fewer than 1GB of records for tests.
    @VisibleForTesting
    GenSortDataGenerator(
            List<ScaleAndRecords> scaleAndRecords,
            SparkSession spark,
            FileSystem destinationFileSystem,
            ParquetTransformer parquetTransformer,
            BenchmarkPaths paths,
            TableRegistration registration,
            Path tempWorkingDir,
            ExecutorService dataGeneratorThreadPool) {
        this.spark = spark;
        this.destinationFileSystem = destinationFileSystem;
        this.parquetTransformer = parquetTransformer;
        this.paths = paths;
        this.registration = registration;
        this.tempWorkingDir = tempWorkingDir;
        this.scaleAndRecords = scaleAndRecords;
        this.dataGeneratorThreadPool = MoreExecutors.listeningDecorator(dataGeneratorThreadPool);
    }

    @Override
    public void generate() {
        spark.sparkContext().setJobDescription("gensort-data-generation");
        try {
            if (tempWorkingDir.toFile().isDirectory()) {
                FileUtils.deleteDirectory(tempWorkingDir.toFile());
            }
            if (!tempWorkingDir.toFile().mkdirs()) {
                throw new IllegalStateException(
                        String.format("Could not create temporary work directory at %s", tempWorkingDir));
            }

            Path genSortBinaryPath = extractBinary();
            log.info("Extracted gensort binary: {}", SafeArg.of("genSortBinaryPath", genSortBinaryPath));
            Path dataDir = Files.createDirectory(tempWorkingDir.resolve(GENSORT_DATA_DIR_NAME));

            scaleAndRecords.forEach(scaleAndRecords -> {
                int scale = scaleAndRecords.scale();
                long totalNumRecords = scaleAndRecords.numRecords();

                // We are estimating the number of records anyway, so it's fine if we generate an extra partition
                // in some cases.
                long numberOfPartitions = (totalNumRecords / RECORDS_PER_PARTITION) + 1;
                LongStream.range(0, numberOfPartitions)
                        .mapToObj(partitionIndex ->
                                generateData(dataDir, scaleAndRecords, partitionIndex, genSortBinaryPath))
                        .collect(Collectors.toList()) // Always collect to force kick off all tasks
                        .forEach(MoreFutures::join);

                StructType schema = DataTypes.createStructType(
                        ImmutableList.of(DataTypes.createStructField("record", DataTypes.StringType, false)));
                String destinationPath = Paths.get(paths.parquetDir(scale), GENERATED_DATA_FILE_NAME)
                        .toString();

                Set<String> generatedCsvPaths = LongStream.range(0, numberOfPartitions)
                        .mapToObj(partitionIndex -> paths.tableCsvFile(scale, GENERATED_DATA_FILE_NAME, partitionIndex))
                        .collect(Collectors.toSet());
                parquetTransformer.transform(spark, schema, generatedCsvPaths, destinationPath, "\n");
                registration.registerTable("gensort_data", schema, scale);
            });
        } catch (IOException e) {
            throw new SafeRuntimeException("IOException while setting up gensort binary", e);
        } finally {
            try {
                FileUtils.deleteDirectory(tempWorkingDir.toFile());
            } catch (IOException e) {
                log.warn(
                        "Failed to delete temporary working directory.",
                        SafeArg.of("tempWorkingDir", tempWorkingDir),
                        e);
            }
        }
    }

    private ListenableFuture<?> generateData(
            Path dataDir, ScaleAndRecords scaleAndRecords, long partitionIndex, Path genSortBinaryPath) {
        int scale = scaleAndRecords.scale();
        long totalNumRecords = scaleAndRecords.numRecords();
        return dataGeneratorThreadPool.submit(() -> {
            Process gensortProcess;
            try {
                String[] command = {
                    genSortBinaryPath.toFile().getAbsolutePath(),
                    "-b" + partitionIndex * RECORDS_PER_PARTITION,
                    "-a",
                    Long.toString(Math.min(RECORDS_PER_PARTITION, totalNumRecords)),
                    dataDir.resolve(GENERATED_DATA_FILE_NAME + "_" + partitionIndex + ".csv")
                            .toAbsolutePath()
                            .toString()
                };
                gensortProcess = new ProcessBuilder()
                        .command(command)
                        .inheritIO()
                        .directory(genSortBinaryPath.toFile().getParentFile())
                        .start();
            } catch (IOException e) {
                throw new SafeRuntimeException("IOException while running gensort program", e);
            }

            int returnCode;
            try {
                returnCode = gensortProcess.waitFor();
            } catch (InterruptedException e) {
                throw new SafeRuntimeException("Thread was interrupted while waiting for gensort process", e);
            }
            if (returnCode != 0) {
                throw new IllegalStateException(String.format("genSort failed with return code %d", returnCode));
            }
            log.info(
                    "Finished running gensort for scale {} and partition {}",
                    SafeArg.of("scale", scale),
                    SafeArg.of("partitionIndex", partitionIndex));
            DataGenUtils.uploadFiles(
                    destinationFileSystem,
                    paths.csvDir(scale),
                    dataDir.toFile(),
                    MoreExecutors.newDirectExecutorService());
        });
    }

    private long estimateNumRecords(int scale) {
        return (scale * BYTES_PER_GB) / BYTES_PER_RECORD;
    }

    private Path extractBinary() throws IOException {
        if (SystemUtils.IS_OS_WINDOWS) {
            throw new UnsupportedOperationException("Cannot generate data using Windows.");
        }
        Path binaryPath;
        Path genSortBinDir = Files.createDirectory(tempWorkingDir.resolve(GENSORT_BIN_DIR_NAME));
        if (SystemUtils.IS_OS_MAC) {
            FileUtils.copyFileToDirectory(GEN_SORT_MACOS_PATH.toFile(), genSortBinDir.toFile());
            binaryPath = Paths.get(
                    genSortBinDir.toString(), GEN_SORT_MACOS_PATH.getFileName().toString());
        } else {
            binaryPath =
                    DataGenUtils.extractBinary(GENSORT_TGZ_LINUX_PATH, GENSORT_LINUX_BINARY_FILE_NAME, genSortBinDir);
        }

        DataGenUtils.makeFileExecutable(binaryPath);
        return binaryPath;
    }

    @Value.Immutable
    public interface ScaleAndRecords {
        int scale();

        long numRecords();

        class Builder extends ImmutableScaleAndRecords.Builder {}

        static Builder builder() {
            return new Builder();
        }
    }
}
