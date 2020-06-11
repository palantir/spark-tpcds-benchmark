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

import java.io.IOException;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public final class ConfigDeserializationTest {
    @Test
    public void testDeserializesConfig() throws IOException {
        // should not throw.
        BenchmarkRunnerConfig.parse(Paths.get("var/conf/config.yml"));
    }
}
