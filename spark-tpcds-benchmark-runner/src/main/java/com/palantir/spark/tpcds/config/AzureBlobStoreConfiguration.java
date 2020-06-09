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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import com.palantir.spark.tpcds.immutables.ImmutablesConfigStyle;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesConfigStyle
@JsonDeserialize(as = ImmutableAzureBlobStoreConfiguration.class)
public abstract class AzureBlobStoreConfiguration extends FilesystemConfiguration {
    @Override
    public final String type() {
        return FilesystemConfiguration.AZURE_BLOB_STORE;
    }

    public abstract String accountName();

    public abstract String accessKey();

    public abstract Optional<String> workingDirectory();

    private String realWorkingDirectory() {
        return StringUtils.strip(workingDirectory().orElse("spark-benchmark"), "/");
    }

    public final String accessKeyPropertyName() {
        return "fs.azure.account.key." + accountName() + ".blob.core.windows.net";
    }

    @Override
    public final String baseUri() {
        return "wasbs://data@" + accountName() + ".blob.core.windows.net/" + realWorkingDirectory();
    }

    @Override
    public final Map<String, String> toHadoopConf() {
        return ImmutableMap.<String, String>builder()
                .put(accessKeyPropertyName(), accessKey())
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends ImmutableAzureBlobStoreConfiguration.Builder {}
}
