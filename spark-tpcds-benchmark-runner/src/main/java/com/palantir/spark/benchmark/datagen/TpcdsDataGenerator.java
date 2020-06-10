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

package com.palantir.spark.benchmark.datagen;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.spark.benchmark.constants.TpcdsTable;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import com.palantir.spark.benchmark.schemas.Schemas;
import com.palantir.spark.benchmark.util.DataGenUtils;
import com.palantir.spark.benchmark.util.MoreFutures;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TpcdsDataGenerator {
    private static final Logger log = LoggerFactory.getLogger(TpcdsDataGenerator.class);

    private static final Path DSDGEN_TGZ_MACOS_PATH = Paths.get("service", "bin", "tpcds", "tpcds_osx.tgz");
    private static final Path DSDGEN_TGZ_LINUX_PATH = Paths.get("service", "bin", "tpcds", "tpcds_linux.tgz");
    private static final String TPCDS_BIN_DIR_NAME = "tpcds-bin";
    private static final String DSDGEN_BINARY_FILE_NAME = "dsdgen";

    private final Path dsdgenWorkLocalDir;
    private final List<Integer> dataScalesGb;
    private final boolean shouldOverwriteData;
    private final FileSystem destinationFileSystem;
    private final ParquetTransformer parquetTransformer;
    private final SparkSession spark;
    private final BenchmarkPaths paths;
    private final Schemas schemas;
    private final ListeningExecutorService dataGeneratorThreadPool;

    public TpcdsDataGenerator(
            Path dsdgenWorkLocalDir,
            List<Integer> dataScalesGb,
            boolean shouldOverwriteData,
            FileSystem destinationFileSystem,
            ParquetTransformer parquetTransformer,
            SparkSession spark,
            BenchmarkPaths paths,
            Schemas schemas,
            ExecutorService dataGeneratorThreadPool) {
        this.dsdgenWorkLocalDir = dsdgenWorkLocalDir;
        this.dataScalesGb = dataScalesGb;
        this.shouldOverwriteData = shouldOverwriteData;
        this.destinationFileSystem = destinationFileSystem;
        this.parquetTransformer = parquetTransformer;
        this.spark = spark;
        this.paths = paths;
        this.schemas = schemas;
        this.dataGeneratorThreadPool = MoreExecutors.listeningDecorator(dataGeneratorThreadPool);
    }

    public void generateData() throws IOException {
        spark.sparkContext().setJobDescription("data-generation");
        Path tempDir = dsdgenWorkLocalDir;
        if (tempDir.toFile().isDirectory()) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
        if (!tempDir.toFile().mkdirs()) {
            throw new IllegalStateException(String.format("Could not create dsdgen work directory at %s", tempDir));
        }
        tempDir.toFile().deleteOnExit();
        try {
            final Path dsdgenFile = extractTpcdsBinary(tempDir);
            dataScalesGb.stream()
                    .map(scale -> generateAndUploadDataForScale(scale, tempDir, dsdgenFile))
                    .collect(Collectors.toList()) // Always collect to force kick off all tasks
                    .forEach(MoreFutures::join);
        } catch (Exception e) {
            try {
                dataGeneratorThreadPool.shutdownNow();
            } catch (Exception e2) {
                log.warn("Error occurred while shutting down the data generator thread pool.", e);
            }
            throw e;
        } finally {
            try {
                FileUtils.deleteDirectory(tempDir.toFile());
            } catch (IOException e) {
                log.warn("Failed to delete temporary working directory.", SafeArg.of("dsdgenWorkDir", tempDir), e);
            }
        }
    }

    private ListenableFuture<?> generateAndUploadDataForScale(int scale, Path tempDir, Path resolvedDsdgenFile) {
        ListenableFuture<?> uploadDataForScaleTask = dataGeneratorThreadPool.submit(() -> {
            try {
                invalidateHashesIfNecessary(scale);
                generateAndUploadCsv(scale, tempDir, resolvedDsdgenFile);
                saveTablesAsParquet(scale);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        });
        uploadDataForScaleTask.addListener(
                () -> log.info("Finished uploading data for data at scale {}.", SafeArg.of("scale", scale)),
                dataGeneratorThreadPool);
        return uploadDataForScaleTask;
    }

    private void generateAndUploadCsv(int scale, Path tempDir, Path resolvedDsdgenFile)
            throws IOException, InterruptedException {
        org.apache.hadoop.fs.Path rootDataPath = new org.apache.hadoop.fs.Path(paths.csvDir(scale));
        if (!shouldGenerateData(scale, rootDataPath)) {
            return;
        }
        File tpcdsTempDir = new File(tempDir.resolve("tpcds-data").toFile(), Integer.toString(scale));
        if (!tpcdsTempDir.mkdirs()) {
            throw new IllegalStateException(
                    String.format("Failed to make tpcds temporary data dir at %s", tpcdsTempDir));
        }
        String[] dsdgenCommand = {
            resolvedDsdgenFile.toFile().getAbsolutePath(),
            "-DIR",
            tpcdsTempDir.getAbsolutePath(),
            "-SCALE",
            Integer.toString(scale),
            "-SUFFIX",
            ".csv",
            "-DELIMITER",
            "|"
        };
        Process dsdgenProcess = new ProcessBuilder()
                .command(dsdgenCommand)
                .inheritIO()
                .directory(resolvedDsdgenFile.toFile().getParentFile())
                .start();
        int returnCode = dsdgenProcess.waitFor();
        if (returnCode != 0) {
            throw new IllegalStateException(String.format("Dsdgen failed with return code %d", returnCode));
        }
        log.info("Finished running dsdgen for data scale {}.", SafeArg.of("scale", scale));
        log.info("Uploading tpcds data from location {}.", SafeArg.of("localLocation", tpcdsTempDir.getAbsolutePath()));
        DataGenUtils.uploadFiles(destinationFileSystem, rootDataPath.toString(), tpcdsTempDir, dataGeneratorThreadPool);
    }

    private void invalidateHashesIfNecessary(int scale) throws IOException {
        // If overwriting, invalidate the previous correctness results.
        org.apache.hadoop.fs.Path correctnessHashesRoot =
                new org.apache.hadoop.fs.Path(paths.experimentCorrectnessHashesRoot(scale));
        if (destinationFileSystem.exists(correctnessHashesRoot)
                && shouldOverwriteData
                && !destinationFileSystem.delete(correctnessHashesRoot, true)) {
            throw new IllegalStateException(String.format(
                    "Failed to clear the correctness hashes result directory at %s.", correctnessHashesRoot));
        }
    }

    private void saveTablesAsParquet(int scale) {
        Stream.of(TpcdsTable.values())
                .filter(table -> {
                    String tableParquetLocation = paths.tableParquetLocation(scale, table);
                    org.apache.hadoop.fs.Path tableParquetLocationPath =
                            new org.apache.hadoop.fs.Path(tableParquetLocation);
                    return shouldGenerateData(scale, tableParquetLocationPath);
                })
                .map(table -> {
                    ListenableFuture<?> saveAsParquetTask = dataGeneratorThreadPool.submit(() -> {
                        parquetTransformer.transform(
                                spark,
                                schemas.getSchema(table),
                                ImmutableSet.of(paths.tableCsvFile(scale, table)),
                                paths.tableParquetLocation(scale, table),
                                "|");
                    });
                    saveAsParquetTask.addListener(
                            () -> log.info(
                                    "Saved a table {} as parquet at scale {}.",
                                    SafeArg.of("table", table),
                                    SafeArg.of("scale", scale)),
                            dataGeneratorThreadPool);
                    return saveAsParquetTask;
                })
                .collect(Collectors.toList())
                .forEach(MoreFutures::join);
    }

    private boolean shouldGenerateData(int scale, org.apache.hadoop.fs.Path tableParquetLocationPath) {
        try {
            if (!destinationFileSystem.exists(tableParquetLocationPath) || shouldOverwriteData) {
                if (destinationFileSystem.isDirectory(tableParquetLocationPath)
                        && !destinationFileSystem.delete(tableParquetLocationPath, true)) {
                    throw new IllegalStateException(
                            String.format("Failed to clear data file directory at %s.", tableParquetLocationPath));
                }
            } else {
                log.info(
                        "Not overwriting data at path {} for the given scale of {}.",
                        SafeArg.of("dataPath", tableParquetLocationPath),
                        SafeArg.of("dataScale", scale));
                return false;
            }
        } catch (IOException e) {
            throw new SafeRuntimeException(e);
        }
        return true;
    }

    private Path extractTpcdsBinary(Path tempDir) throws IOException {
        Path dsdgenTgzPath = findDsdgenTgz();
        Path dsdgenBinDir = Files.createDirectory(tempDir.resolve(TPCDS_BIN_DIR_NAME));
        Path dsdgenFile = DataGenUtils.extractBinary(dsdgenTgzPath, DSDGEN_BINARY_FILE_NAME, dsdgenBinDir);
        DataGenUtils.makeFileExecutable(dsdgenFile);
        return dsdgenFile;
    }

    private Path findDsdgenTgz() throws FileNotFoundException {
        Path dsdgenTgzPath;
        if (SystemUtils.IS_OS_WINDOWS) {
            throw new UnsupportedOperationException("Cannot generate data using Windows.");
        } else if (SystemUtils.IS_OS_MAC) {
            dsdgenTgzPath = DSDGEN_TGZ_MACOS_PATH;
        } else {
            dsdgenTgzPath = DSDGEN_TGZ_LINUX_PATH;
        }
        if (!dsdgenTgzPath.toFile().isFile()) {
            throw new FileNotFoundException(String.format(
                    "Dsdgen tarball not found at %s; was this benchmark runner" + " packaged correctly?",
                    dsdgenTgzPath));
        }
        return dsdgenTgzPath;
    }
}
