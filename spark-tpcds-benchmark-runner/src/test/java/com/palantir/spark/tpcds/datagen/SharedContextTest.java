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

import com.palantir.spark.tpcds.config.HadoopConfiguration;
import java.io.File;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.spark_project.guava.io.Files;

public abstract class SharedContextTest {
    static final File TEMPORARY_FOLDER = Files.createTempDir();

    static final HadoopConfiguration TEST_HADOOP_CONFIGURATION = HadoopConfiguration.builder()
            // .putHadoopConf("fs.defaultFS", "file://" + TEMPORARY_FOLDER.getAbsolutePath())
            .build();

    SparkSession sparkSession;

    @BeforeEach
    public void beforeEach() {
        sparkSession = SparkSession.builder()
                .appName("tests")
                .master("local")
                .config("spark.ui.enabled", false)
                .config("spark.ui.showConsoleProgress", false)
                .getOrCreate();
    }
}
