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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.tpcds.paths.BenchmarkPaths;
import com.palantir.spark.tpcds.registration.TableRegistration;
import com.palantir.spark.tpcds.util.DataGenUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GenSortDataGenerator implements SortDataGenerator {
    private static final Logger log = LoggerFactory.getLogger(GenSortDataGenerator.class);

    private static final int BYTES_PER_RECORD = 100;

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
    private final int scale;

    public GenSortDataGenerator(
            SparkSession spark,
            FileSystem destinationFileSystem,
            ParquetTransformer parquetTransformer,
            BenchmarkPaths paths,
            TableRegistration registration,
            Path tempWorkingDir,
            int scale) {
        this.spark = spark;
        this.destinationFileSystem = destinationFileSystem;
        this.parquetTransformer = parquetTransformer;
        this.paths = paths;
        this.registration = registration;
        this.tempWorkingDir = tempWorkingDir;
        this.scale = scale;
    }

    @Override
    public void generate() throws Exception {
        spark.sparkContext().setJobDescription("gensort-data-generation");
        if (tempWorkingDir.toFile().isDirectory()) {
            FileUtils.deleteDirectory(tempWorkingDir.toFile());
        }
        if (!tempWorkingDir.toFile().mkdirs()) {
            throw new IllegalStateException(
                    String.format("Could not create temporary work directory at %s", tempWorkingDir));
        }
        try {
            final Path genSortFilePath = extractBinary();
            log.info("Extracted gensort binary: {}", SafeArg.of("genSortFilePath", genSortFilePath));
            Path dataDir = Files.createDirectory(tempWorkingDir.resolve(GENSORT_DATA_DIR_NAME));
            Process gensortProcess = new ProcessBuilder()
                    .command(
                            genSortFilePath.toFile().getAbsolutePath(),
                            "-a",
                            Long.toString(estimateNumRecords()),
                            dataDir.resolve(GENERATED_DATA_FILE_NAME + ".csv")
                                    .toAbsolutePath()
                                    .toString())
                    .inheritIO()
                    .directory(genSortFilePath.toFile().getParentFile())
                    .start();
            int returnCode = gensortProcess.waitFor();
            if (returnCode != 0) {
                throw new IllegalStateException(String.format("genSort failed with return code %d", returnCode));
            }
            log.info("Finished running gensort");
            DataGenUtils.uploadFiles(
                    destinationFileSystem,
                    paths.csvDir(scale),
                    dataDir.toFile(),
                    MoreExecutors.newDirectExecutorService());
            StructType schema = DataTypes.createStructType(
                    ImmutableList.of(DataTypes.createStructField("record", DataTypes.StringType, false)));
            String destinationPath =
                    Paths.get(paths.parquetDir(scale), GENERATED_DATA_FILE_NAME).toString();
            parquetTransformer.transform(
                    spark, schema, paths.tableCsvFile(scale, GENERATED_DATA_FILE_NAME), destinationPath, "\n");
            registration.registerTable("gensort_data", schema, scale);
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

    private int estimateNumRecords() {
        return (scale * 1024 * 1024) / BYTES_PER_RECORD;
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
}
