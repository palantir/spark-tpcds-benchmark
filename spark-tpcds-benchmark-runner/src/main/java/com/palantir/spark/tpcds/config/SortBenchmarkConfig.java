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

package com.palantir.spark.tpcds.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.palantir.spark.tpcds.immutables.ImmutablesConfigStyle;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesConfigStyle
@JsonDeserialize(as = ImmutableSortBenchmarkConfig.class)
public interface SortBenchmarkConfig {
    @Value.Default
    default boolean enabled() {
        return true;
    }

    @Value.Default
    default long numRecords() {
        return 100_000;
    }

    class Builder extends ImmutableSortBenchmarkConfig.Builder {}

    static Builder builder() {
        return new Builder();
    }
}
