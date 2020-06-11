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

package com.palantir.spark.benchmark.config;

import static com.palantir.logsafe.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.spark.benchmark.immutables.ImmutablesConfigStyle;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesConfigStyle
@JsonDeserialize(as = ImmutableBenchmarkRunnerConfig.class)
public interface BenchmarkRunnerConfig {
    HadoopConfiguration hadoop();

    SparkConfiguration spark();

    List<Integer> dataScalesGb();

    DataGenerationConfiguration dataGeneration();

    BenchmarksConfiguration benchmarks();

    @Value.Check
    default void check() {
        checkArgument(!dataScalesGb().isEmpty(), "Must specify at least one data scale to run benchmarks against.");
        dataScalesGb().forEach(scale -> checkArgument(scale > 0, "All data scales must be positive."));
    }

    static BenchmarkRunnerConfig parse(Path configFile) throws IOException {
        ObjectMapper objectMapper =
                new ObjectMapper(new YAMLFactory()).registerModules(new Jdk8Module(), new GuavaModule());
        return objectMapper.readValue(configFile.toFile(), BenchmarkRunnerConfig.class);
    }

    class Builder extends ImmutableBenchmarkRunnerConfig.Builder {}

    static Builder builder() {
        return new Builder();
    }
}
