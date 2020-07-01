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
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.spark.benchmark.immutables.ImmutablesConfigStyle;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesConfigStyle
@JsonDeserialize(as = ImmutableHadoopConfiguration.class)
public interface HadoopConfiguration {
    List<Path> hadoopConfDirs();

    Map<String, String> hadoopConf();

    String defaultFilesystem();

    Optional<String> metricsFileSystem();

    Map<String, FilesystemConfiguration> filesystems();

    @Value.Derived
    default String defaultFsBaseUriString() {
        return getFilesystemBaseUriOrThrow(defaultFilesystem());
    }

    @Value.Derived
    default String defaultMetricsBaseUriString() {
        return getFilesystemBaseUriOrThrow(metricsFileSystem().orElseGet(this::defaultFilesystem));
    }

    default String getFilesystemBaseUriOrThrow(String filesystemName) {
        return Optional.ofNullable(filesystems().get(filesystemName))
                .orElseThrow(() -> new SafeIllegalArgumentException(
                        "Specified filesystem is not configured", SafeArg.of("filesystem", filesystemName)))
                .baseUri();
    }

    @Value.Derived
    default URI defaultFsBaseUri() {
        return new org.apache.hadoop.fs.Path(defaultFsBaseUriString()).toUri();
    }

    @Value.Derived
    default Configuration toHadoopConf() {
        Configuration hadoopConf = new Configuration();

        // first load the values from xml in the provided directories
        for (Path hadoopConfDir : hadoopConfDirs()) {
            try {
                hadoopConf = loadConfFromFile(hadoopConf, hadoopConfDir.toFile());
            } catch (MalformedURLException e) {
                throw new SafeRuntimeException("Malformed URL when parsing Hadoop config", e);
            }
        }

        // then load the free-form config overrides
        hadoopConf().forEach(hadoopConf::set);

        // finally, apply the filesystem settings
        KeyedStream.ofEntries(
                        filesystems().values().stream().flatMap(fsConf -> fsConf.toHadoopConf().entrySet().stream()))
                .collectToMap()
                .forEach(hadoopConf::set);
        hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, defaultFsBaseUriString());
        return hadoopConf;
    }

    static Configuration loadConfFromFile(Configuration conf, File confFile) throws MalformedURLException {
        Configuration resolvedConfiguration = conf;
        if (confFile.isDirectory()) {
            for (File child : Optional.ofNullable(confFile.listFiles()).orElse(new File[0])) {
                resolvedConfiguration = loadConfFromFile(resolvedConfiguration, child);
            }
        } else if (confFile.isFile() && confFile.getName().endsWith(".xml")) {
            resolvedConfiguration.addResource(confFile.toURI().toURL());
        }
        return resolvedConfiguration;
    }

    class Builder extends ImmutableHadoopConfiguration.Builder {}

    static Builder builder() {
        return new Builder();
    }
}
