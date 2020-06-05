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

import com.palantir.spark.tpcds.datagen.DefaultParquetTransformer;
import com.palantir.spark.tpcds.datagen.GenSortDataGenerator;
import com.palantir.spark.tpcds.paths.BenchmarkPaths;
import com.palantir.spark.tpcds.queries.SortBenchmarkQuery;
import com.palantir.spark.tpcds.registration.TableRegistration;
import com.palantir.spark.tpcds.schemas.Schemas;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

public final class GenSortTest extends AbstractLocalSparkTest {
    @Test
    public void testGeneratesData() throws Exception {
        Path workingDir = createTemporaryWorkingDir("working_dir");
        Path destinationDataDirectory = createTemporaryWorkingDir("data");
        int scale = 1;

        String fullyQualifiedDestinationDir =
                "file://" + destinationDataDirectory.toFile().getAbsolutePath();
        FileSystem dataFileSystem =
                FileSystem.get(URI.create(fullyQualifiedDestinationDir), TEST_HADOOP_CONFIGURATION.toHadoopConf());

        BenchmarkPaths paths =
                new BenchmarkPaths(destinationDataDirectory.toFile().getAbsolutePath());
        GenSortDataGenerator genSortDataGenerator = new GenSortDataGenerator(
                sparkSession,
                dataFileSystem,
                new DefaultParquetTransformer(), // test that our schema works by copying for real.
                paths,
                new TableRegistration(paths, dataFileSystem, sparkSession, new Schemas()),
                workingDir,
                scale);
        genSortDataGenerator.generate();

        List<String> generatedLines = read(Paths.get(paths.tableCsvFile(scale, "gensort_data")), "csv");
        assertThat(generatedLines).hasSize(10485); // (1GB / 100 bytes)

        List<String> copiedParquet = read(Paths.get(paths.tableParquetLocation(scale, "gensort_data")), "parquet");
        assertThat(copiedParquet).hasSameElementsAs(generatedLines);

        SortBenchmarkQuery query = new SortBenchmarkQuery(sparkSession);
        // Should not throw. We can't assert sortedness since the data could be saved in multiple partitions.
        query.save(paths.experimentResultLocation(scale, "gensort"));

        // Clean up
        FileUtils.deleteDirectory(destinationDataDirectory.toFile());
    }

    private List<String> read(Path path, String format) {
        return sparkSession
                .read()
                .option("delimiter", "\n")
                .format(format)
                .load(path.toString())
                .collectAsList()
                .stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());
    }
}
