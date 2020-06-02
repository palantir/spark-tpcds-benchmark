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
import com.palantir.spark.tpcds.immutables.ImmutablesConfigStyle;
import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesConfigStyle
@JsonDeserialize(as = ImmutableHadoopConfiguration.class)
public interface HadoopConfiguration {
    List<Path> hadoopConfDirs();

    Map<String, String> hadoopConf();

    @Value.Derived
    default Configuration toHadoopConf() {
        Configuration hadoopConf = new Configuration();
        for (Path hadoopConfDir : hadoopConfDirs()) {
            try {
                hadoopConf = loadConfFromFile(hadoopConf, hadoopConfDir.toFile());
            } catch (MalformedURLException e) {
                throw new RuntimeException("Malformed URL when parsing Hadoop config", e);
            }
        }
        hadoopConf().forEach(hadoopConf::set);
        return hadoopConf;
    }

    static Configuration loadConfFromFile(Configuration conf, File confFile) throws MalformedURLException {
        Configuration resolvedConfiguration = conf;
        if (confFile.isDirectory()) {
            for (File child : Optional.ofNullable(confFile.listFiles()).orElse(new File[0])) {
                resolvedConfiguration = loadConfFromFile(resolvedConfiguration, child);
            }
        } else if (confFile.isFile() && confFile.getName().endsWith(".xml")) {
            resolvedConfiguration.addResource(confFile.toURL());
        }
        return resolvedConfiguration;
    }

    class Builder extends ImmutableHadoopConfiguration.Builder {}

    static Builder builder() {
        return new Builder();
    }
}
