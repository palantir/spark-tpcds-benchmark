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

package com.palantir.spark.benchmark.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import com.palantir.spark.benchmark.immutables.ImmutablesConfigStyle;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesConfigStyle
@JsonDeserialize(as = ImmutableSimpleFilesystemConfiguration.class)
public abstract class SimpleFilesystemConfiguration extends FilesystemConfiguration {
    @Override
    public final String type() {
        return FilesystemConfiguration.SIMPLE_TYPE;
    }

    @Override
    public final Map<String, String> toHadoopConf() {
        return ImmutableMap.of();
    }

    public static SimpleFilesystemConfiguration of(String baseUri) {
        return ImmutableSimpleFilesystemConfiguration.builder().baseUri(baseUri).build();
    }
}
