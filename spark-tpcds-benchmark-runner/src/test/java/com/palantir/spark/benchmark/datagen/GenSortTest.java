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

package com.palantir.spark.benchmark.datagen;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.spark.benchmark.config.HadoopConfiguration;
import com.palantir.spark.benchmark.datagen.GenSortDataGenerator.ScaleAndRecords;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import com.palantir.spark.benchmark.queries.SortBenchmarkQuery;
import com.palantir.spark.benchmark.registration.TableRegistration;
import com.palantir.spark.benchmark.schemas.Schemas;
import com.palantir.spark.benchmark.util.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

public final class GenSortTest extends AbstractLocalSparkTest {
    @Test
    public void testGeneratesData() throws Exception {
        Path workingDir = createTemporaryWorkingDir("working_dir");
        Path destinationDataDirectory = createTemporaryWorkingDir("data");
        HadoopConfiguration hadoopConfiguration = getHadoopConfiguration(destinationDataDirectory);
        FileSystem dataFileSystem = FileSystems.createFileSystem(
                hadoopConfiguration.defaultFsBaseUri(), hadoopConfiguration.toHadoopConf());

        BenchmarkPaths paths = new BenchmarkPaths("foo");
        int scale = 1;
        int numRecords = 100;
        Schemas schemas = new Schemas();
        TableRegistration tableRegistration = new TableRegistration(paths, dataFileSystem, sparkSession, schemas);

        GenSortDataGenerator genSortDataGenerator = new GenSortDataGenerator(
                ImmutableList.of(ScaleAndRecords.builder()
                        .scale(scale)
                        .numRecords(numRecords)
                        .build()),
                sparkSession,
                dataFileSystem,
                new DefaultParquetTransformer(), // test that our schema works by copying for real.
                paths,
                schemas,
                workingDir,
                true,
                MoreExecutors.newDirectExecutorService());
        genSortDataGenerator.generate();
        tableRegistration.registerGensortTable(scale);

        List<String> generatedLines = read(
                Paths.get(
                        hadoopConfiguration.defaultFsBaseUri().getPath(), paths.tableCsvFile(scale, "gensort_data", 0)),
                "csv");
        assertThat(generatedLines).hasSize(numRecords);

        List<String> copiedParquet = read(
                Paths.get(
                        hadoopConfiguration.defaultFsBaseUri().getPath(),
                        paths.tableParquetLocation(scale, "gensort_data")),
                "parquet");
        assertThat(copiedParquet).hasSameElementsAs(generatedLines);

        SortBenchmarkQuery query = new SortBenchmarkQuery(sparkSession);
        // Should not throw. We can't assert sortedness since the data could be saved in multiple partitions.
        query.save(paths.experimentResultLocation(scale, UUID.randomUUID(), "gensort"));
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
