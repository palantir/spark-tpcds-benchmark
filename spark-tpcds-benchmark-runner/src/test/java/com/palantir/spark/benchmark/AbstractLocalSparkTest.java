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

import com.palantir.spark.benchmark.config.HadoopConfiguration;
import com.palantir.spark.benchmark.config.SimpleFilesystemConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractLocalSparkTest {
    protected SparkSession sparkSession;

    @BeforeEach
    public void beforeEach() {
        sparkSession = SparkSession.builder()
                .appName("tests")
                .master("local")
                .config("spark.ui.enabled", false)
                .config("spark.ui.showConsoleProgress", false)
                // Adding these here so that we can make sure that these jars are in the classpath.
                .config(
                        "spark.sql.sources.commitProtocolClass",
                        "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
                .config(
                        "spark.sql.parquet.output.committer.class",
                        "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
                .getOrCreate();
    }

    protected final Path createTemporaryWorkingDir(String prefix) throws IOException {
        Path directory = Files.createDirectory(Paths.get("/tmp", prefix + "_" + UUID.randomUUID()));
        directory.toFile().deleteOnExit();
        return directory;
    }

    protected final HadoopConfiguration getHadoopConfiguration(Path destinationDataDirectory) {
        String fullyQualifiedDestinationDir =
                "file://" + destinationDataDirectory.toFile().getAbsolutePath();
        return HadoopConfiguration.builder()
                .defaultFilesystem("local")
                .putFilesystems("local", SimpleFilesystemConfiguration.of(fullyQualifiedDestinationDir))
                .build();
    }
}
