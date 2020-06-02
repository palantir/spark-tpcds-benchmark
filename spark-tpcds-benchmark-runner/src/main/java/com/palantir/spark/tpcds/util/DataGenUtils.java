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

package com.palantir.spark.tpcds.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

public final class DataGenUtils {
    private DataGenUtils() {}

    public static void makeFileExecutable(Path genSortFile) {
        if (!genSortFile.toFile().canExecute() && !genSortFile.toFile().setExecutable(true, true)) {
            throw new IllegalStateException(
                    String.format("Could not make the gensort binary at %s executable.", genSortFile));
        }
    }

    /**
     * Extracts the tar ball at tgzPath into binDir. Returns the file inside the tgz
     * that is equal to binaryName. If no file exists that matches binaryName, FileNotFoundException is thrown.
     */
    public static Path extractBinary(Path tgzPath, String binaryName, Path binDir) throws IOException {
        Path dsdgenFile = null;
        try (FileInputStream rawTarInput = new FileInputStream(tgzPath.toFile());
                BufferedInputStream bufferingInput = new BufferedInputStream(rawTarInput);
                GZIPInputStream decompressingInput = new GZIPInputStream(bufferingInput);
                TarArchiveInputStream untarringInput = new TarArchiveInputStream(decompressingInput)) {
            TarArchiveEntry entry;
            while ((entry = untarringInput.getNextTarEntry()) != null) {
                Path outputPath = binDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectory(outputPath);
                } else {
                    try (FileOutputStream output = new FileOutputStream(outputPath.toFile())) {
                        IOUtils.copy(untarringInput, output);
                    }
                }
                if (outputPath.toFile().getName().equals(binaryName)) {
                    dsdgenFile = outputPath;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to extract tpcds tar at %s",
                            tgzPath.toFile().getAbsolutePath()),
                    e);
        }
        if (dsdgenFile == null) {
            throw new FileNotFoundException(
                    "Dsdgen binary was not found in the tarball; was this benchmark runner packaged correctly?");
        }
        return dsdgenFile;
    }
}
