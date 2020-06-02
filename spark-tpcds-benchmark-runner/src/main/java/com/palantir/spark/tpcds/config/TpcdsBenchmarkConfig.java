/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.spark.tpcds.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.logsafe.Preconditions;
import com.palantir.spark.tpcds.immutables.ImmutablesConfigStyle;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesConfigStyle
@JsonDeserialize(as = ImmutableTpcdsBenchmarkConfig.class)
public interface TpcdsBenchmarkConfig {
    ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory()).registerModules(new Jdk8Module(), new GuavaModule());

    SparkConfiguration spark();

    HadoopConfiguration hadoop();

    Path dsdgenWorkLocalDir();

    String testDataDir();

    boolean generateData();

    @Value.Default
    default boolean overwriteData() {
        return false;
    }

    List<Integer> dataScalesGb();

    @Value.Default
    default int dataGenerationParallelism() {
        return 5;
    }

    @Value.Default
    default int iterations() {
        return 1;
    }

    SortBenchmarkConfig sort();

    @Value.Default
    default boolean excludeSqlQueries() {
        return false;
    }

    @Value.Check
    default void check() {
        Preconditions.checkArgument(iterations() >= 0, "Iterations must be non-negative.");
        Preconditions.checkArgument(dataGenerationParallelism() > 0, "Data generation parallelism must be positive.");
        Preconditions.checkArgument(
                !dataScalesGb().isEmpty(), "Must specify at least one data scale to run benchmarks against.");
        dataScalesGb().forEach(scale -> {
            Preconditions.checkArgument(scale > 0, "All data scales must be positive.");
        });
    }

    static TpcdsBenchmarkConfig parse(Path configFile) throws IOException {
        return MAPPER.readValue(configFile.toFile(), TpcdsBenchmarkConfig.class);
    }
}
