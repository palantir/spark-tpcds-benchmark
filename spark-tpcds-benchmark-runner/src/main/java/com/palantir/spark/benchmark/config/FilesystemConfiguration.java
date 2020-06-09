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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = SimpleFilesystemConfiguration.class, name = FilesystemConfiguration.SIMPLE_TYPE),
    @JsonSubTypes.Type(value = S3Configuration.class, name = FilesystemConfiguration.AMAZON_S3_TYPE),
    @JsonSubTypes.Type(value = AzureBlobStoreConfiguration.class, name = FilesystemConfiguration.AZURE_BLOB_STORE)
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class FilesystemConfiguration {
    public static final String SIMPLE_TYPE = "simple";
    public static final String AMAZON_S3_TYPE = "s3a";
    public static final String AZURE_BLOB_STORE = "azure";

    @JsonProperty("type")
    public abstract String type();

    public abstract String baseUri();

    public abstract Map<String, String> toHadoopConf();
}
