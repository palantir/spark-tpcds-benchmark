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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.spark.tpcds.paths.TpcdsPaths;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

public final class GenSortDataGeneratorTest extends AbstractLocalSparkTest {
    @Test
    public void testGeneratesData() throws Exception {
        Path workingDir = createTemporaryWorkingDir("working_dir");
        Path destinationDataDirectory = createTemporaryWorkingDir("data");

        String fullyQualifiedDestinationDir =
                "file://" + destinationDataDirectory.toFile().getAbsolutePath();
        FileSystem dataFileSystem =
                FileSystem.get(URI.create(fullyQualifiedDestinationDir), TEST_HADOOP_CONFIGURATION.toHadoopConf());

        GenSortDataGenerator genSortDataGenerator = new GenSortDataGenerator(
                sparkSession, dataFileSystem, new TpcdsPaths(fullyQualifiedDestinationDir), workingDir, 100);
        genSortDataGenerator.generate();
        try (Stream<String> generatedLines = Files.lines(
                Paths.get(destinationDataDirectory.toString(), "gensort_data", "raw_csv", "gensort-data-file"))) {
            assertThat(generatedLines.count()).isEqualTo(100);
        }
    }
}
