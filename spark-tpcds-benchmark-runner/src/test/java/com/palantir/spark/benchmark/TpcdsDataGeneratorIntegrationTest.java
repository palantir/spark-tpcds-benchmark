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

package com.palantir.spark.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.spark.tpcds.datagen.DefaultParquetTransformer;
import com.palantir.spark.tpcds.datagen.TpcdsDataGenerator;
import com.palantir.spark.tpcds.paths.BenchmarkPaths;
import com.palantir.spark.tpcds.schemas.Schemas;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

public final class TpcdsDataGeneratorIntegrationTest extends AbstractLocalSparkTest {
    @Test
    public void testGeneratesAndUploadsData() throws Exception {
        Path workingDir = createTemporaryWorkingDir("working_dir");
        Path destinationDataDirectory = createTemporaryWorkingDir("data");

        String fullyQualifiedDestinationDir =
                "file://" + destinationDataDirectory.toFile().getAbsolutePath();
        FileSystem dataFileSystem =
                FileSystem.get(URI.create(fullyQualifiedDestinationDir), TEST_HADOOP_CONFIGURATION.toHadoopConf());
        int scale = 1;

        BenchmarkPaths paths = new BenchmarkPaths(fullyQualifiedDestinationDir);
        TpcdsDataGenerator generator = new TpcdsDataGenerator(
                workingDir,
                ImmutableList.of(scale),
                false,
                dataFileSystem,
                new DefaultParquetTransformer(),
                sparkSession,
                paths,
                new Schemas(),
                MoreExecutors.newDirectExecutorService());
        generator.generateData();
        try (Stream<Path> generatedCsvFiles = Files.list(Paths.get(paths.csvDir(scale)))
                .filter(path -> path.toString().endsWith(".csv"))) {
            assertThat(generatedCsvFiles.count()).isEqualTo(25);
        }
    }
}
