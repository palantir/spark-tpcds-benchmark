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

package com.palantir.spark.tpcds.datagen;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.tpcds.constants.TpcdsTable;
import com.palantir.spark.tpcds.paths.TpcdsPaths;
import com.palantir.spark.tpcds.schemas.TpcdsSchemas;
import com.palantir.spark.tpcds.util.DataGenUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
    private final FileSystem dataFileSystem;
    private final ParquetCopier parquetCopier;
    private final SparkSession spark;
    private final TpcdsPaths paths;
    private final TpcdsSchemas schemas;
    private final ListeningExecutorService dataGeneratorThreadPool;

    public TpcdsDataGenerator(
            Path dsdgenWorkLocalDir,
            List<Integer> dataScalesGb,
            boolean shouldOverwriteData,
            FileSystem dataFileSystem,
            ParquetCopier parquetCopier,
            SparkSession spark,
            TpcdsPaths paths,
            TpcdsSchemas schemas,
            ExecutorService dataGeneratorThreadPool) {
        this.dsdgenWorkLocalDir = dsdgenWorkLocalDir;
        this.dataScalesGb = dataScalesGb;
        this.shouldOverwriteData = shouldOverwriteData;
        this.dataFileSystem = dataFileSystem;
        this.parquetCopier = parquetCopier;
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
                    .forEach(TpcdsDataGenerator::waitForFuture);
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
                org.apache.hadoop.fs.Path rootDataPath = new org.apache.hadoop.fs.Path(paths.tpcdsCsvDir(scale));
                if (!dataFileSystem.exists(rootDataPath) || shouldOverwriteData) {
                    if (dataFileSystem.isDirectory(rootDataPath) && !dataFileSystem.delete(rootDataPath, true)) {
                        throw new IllegalStateException(
                                String.format("Failed to clear data file directory at %s.", rootDataPath));
                    }
                } else {
                    log.info(
                            "Not overwriting data at path {} for the given scale of {}.",
                            SafeArg.of("dataPath", rootDataPath),
                            SafeArg.of("dataScale", scale));
                    return;
                }
                invalidateHashesIfNecessary(scale);

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
                log.info(
                        "Uploading tpcds data from location {}.",
                        SafeArg.of("localLocation", tpcdsTempDir.getAbsolutePath()));
                DataGenUtils.uploadFiles(dataFileSystem, rootDataPath, tpcdsTempDir, dataGeneratorThreadPool);
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

    private void invalidateHashesIfNecessary(int scale) throws IOException {
        // If overwriting, invalidate the previous correctness results.
        org.apache.hadoop.fs.Path correctnessHashesRoot =
                new org.apache.hadoop.fs.Path(paths.experimentCorrectnessHashesRoot(scale));
        if (dataFileSystem.exists(correctnessHashesRoot)
                && shouldOverwriteData
                && !dataFileSystem.delete(correctnessHashesRoot, true)) {
            throw new IllegalStateException(String.format(
                    "Failed to clear the correctness hashes result directory at %s.", correctnessHashesRoot));
        }
    }

    private void saveTablesAsParquet(int scale) {
        Stream.of(TpcdsTable.values())
                .map(table -> {
                    ListenableFuture<?> saveAsParquetTask = dataGeneratorThreadPool.submit(() -> {
                        parquetCopier.copy(
                                spark,
                                schemas.getSchema(table),
                                paths.tableCsvFile(scale, table),
                                paths.tableParquetLocation(scale, table));
                    });
                    saveAsParquetTask.addListener(
                            () -> {
                                log.info(
                                        "Saved a table {} as parquet at scale {}.",
                                        SafeArg.of("table", table),
                                        SafeArg.of("scale", scale));
                            },
                            dataGeneratorThreadPool);
                    return saveAsParquetTask;
                })
                .collect(Collectors.toList())
                .forEach(TpcdsDataGenerator::waitForFuture);
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

    private static void waitForFuture(ListenableFuture<?> future) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
