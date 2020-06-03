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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
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
                sparkSession,
                dataFileSystem,
                new DefaultParquetTransformer(), // test that our schema works by copying for real.
                new TpcdsPaths(fullyQualifiedDestinationDir),
                workingDir,
                100);
        genSortDataGenerator.generate();

        List<String> generatedLines = read(
                Paths.get(destinationDataDirectory.toString(), "gensort_data", "raw_csv", "gensort-data-file"),
                "csv",
                "\n");
        assertThat(generatedLines).hasSize(100);

        List<String> copiedParquet = read(
                Paths.get(destinationDataDirectory.toString(), "gensort_data", "raw_parquet", "gensort-data-file"),
                "parquet",
                "\n");
        assertThat(copiedParquet).hasSameElementsAs(generatedLines);
    }

    private List<String> read(Path path, String format, String delimiter) {
        return sparkSession
                .read()
                .option("delimiter", delimiter)
                .format(format)
                .load(path.toString())
                .collectAsList()
                .stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());
    }
}
