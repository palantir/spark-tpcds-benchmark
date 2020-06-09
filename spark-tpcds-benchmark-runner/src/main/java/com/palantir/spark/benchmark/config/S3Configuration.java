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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import com.palantir.spark.benchmark.immutables.ImmutablesConfigStyle;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.s3a.Constants;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesConfigStyle
@JsonDeserialize(as = ImmutableS3Configuration.class)
public abstract class S3Configuration extends FilesystemConfiguration {
    @Override
    public final String type() {
        return FilesystemConfiguration.AMAZON_S3_TYPE;
    }

    public abstract Optional<String> accessKey();

    public abstract Optional<String> secretKey();

    @Override
    public final Map<String, String> toHadoopConf() {
        ImmutableMap.Builder<String, String> builder =
                ImmutableMap.<String, String>builder().put(Constants.FAST_UPLOAD, "true");
        accessKey().ifPresent(accessKey -> builder.put(Constants.ACCESS_KEY, accessKey));
        secretKey().ifPresent(secretKey -> builder.put(Constants.SECRET_KEY, secretKey));
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends ImmutableS3Configuration.Builder {}
}
