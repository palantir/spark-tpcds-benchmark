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

import com.palantir.logsafe.SafeArg;
import com.palantir.spark.tpcds.config.TpcdsBenchmarkConfig;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GenSortDataGenerator implements SortDataGenerator {
    private static final Logger log = LoggerFactory.getLogger(GenSortDataGenerator.class);

    private static final Path GEN_SORT_MACOS_PATH = Paths.get("service", "bin", "gensort", "gensort_osx");
    private static final Path GENSORT_TGZ_LINUX_PATH = Paths.get("service", "bin", "gensort", "gensort.tgz");
    private static final String GENSORT_LINUX_BIN_DIR_NAME = "gensort";
    private static final String GENSORT_LINUX_BINARY_FILE_NAME = "gensort";

    private final SparkSession spark;
    private final TpcdsBenchmarkConfig config;

    public GenSortDataGenerator(SparkSession spark, TpcdsBenchmarkConfig config) {
        this.spark = spark;
        this.config = config;
    }

    @Override
    public void generate() throws Exception {
        spark.sparkContext().setJobDescription("data-generation");
        Path tempDir = config.dsdgenWorkLocalDir();
        if (tempDir.toFile().isDirectory()) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
        if (!tempDir.toFile().mkdirs()) {
            throw new IllegalStateException(String.format("Could not create dsdgen work directory at %s", tempDir));
        }
        try {
            final Path genSortFilePath = extractBinary(tempDir);
            log.info("Extracted gensort binary: {}", SafeArg.of("genSortFilePath", genSortFilePath));
            Process gensortProcess = new ProcessBuilder()
                    .command(
                            genSortFilePath.toFile().getAbsolutePath(),
                            "-a",
                            Long.toString(config.sort().numRecords()),
                            "sort-data")
                    .inheritIO()
                    .directory(genSortFilePath.toFile().getParentFile())
                    .start();
            int returnCode = gensortProcess.waitFor();
            if (returnCode != 0) {
                throw new IllegalStateException(String.format("genSort failed with return code %d", returnCode));
            }
            log.info("Finished running gensort");
        } finally {
            try {
                FileUtils.deleteDirectory(tempDir.toFile());
            } catch (IOException e) {
                log.warn("Failed to delete temporary working directory.", SafeArg.of("dsdgenWorkDir", tempDir), e);
            }
        }
    }

    private Path extractBinary(Path tempDir) throws IOException {
        if (SystemUtils.IS_OS_WINDOWS) {
            throw new UnsupportedOperationException("Cannot generate data using Windows.");
        }
        Path binaryPath = unzipAndCopyBinary(tempDir);
        makeFileExecutable(binaryPath);
        return binaryPath;
    }

    private Path unzipAndCopyBinary(Path tempDir) throws IOException {
        Path genSortBinDir = Files.createDirectory(tempDir.resolve(GENSORT_LINUX_BIN_DIR_NAME));
        if (SystemUtils.IS_OS_MAC) {
            FileUtils.copyFileToDirectory(GEN_SORT_MACOS_PATH.toFile(), genSortBinDir.toFile());
            return Paths.get(
                    genSortBinDir.toString(), GEN_SORT_MACOS_PATH.getFileName().toString());
        }

        Path genSortFile = null;
        try (FileInputStream rawTarInput = new FileInputStream(GENSORT_TGZ_LINUX_PATH.toFile());
                BufferedInputStream bufferingInput = new BufferedInputStream(rawTarInput);
                GZIPInputStream decompressingInput = new GZIPInputStream(bufferingInput);
                TarArchiveInputStream untarringInput = new TarArchiveInputStream(decompressingInput)) {
            TarArchiveEntry entry;
            while ((entry = untarringInput.getNextTarEntry()) != null) {
                Path outputPath = genSortBinDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectory(outputPath);
                } else {
                    try (FileOutputStream output = new FileOutputStream(outputPath.toFile())) {
                        IOUtils.copy(untarringInput, output);
                    }
                }
                if (outputPath.toFile().getName().equals(GENSORT_LINUX_BINARY_FILE_NAME)) {
                    genSortFile = outputPath;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to extract tpcds tar at %s",
                            GENSORT_TGZ_LINUX_PATH.toFile().getAbsolutePath()),
                    e);
        }
        if (genSortFile == null) {
            throw new FileNotFoundException(
                    "Gensort binary was not found in the tarball; was this benchmark runner packaged correctly?");
        }
        return genSortFile;
    }

    private void makeFileExecutable(Path genSortFile) {
        if (!genSortFile.toFile().canExecute() && !genSortFile.toFile().setExecutable(true, true)) {
            throw new IllegalStateException(
                    String.format("Could not make the gensort binary at %s executable.", genSortFile));
        }
    }
}
