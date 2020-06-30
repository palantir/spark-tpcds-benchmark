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

package com.palantir.spark.benchmark.datagen;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.spark.benchmark.config.SparkConfiguration;
import com.palantir.spark.benchmark.metrics.BenchmarkMetric;
import com.palantir.spark.benchmark.metrics.BenchmarkMetrics;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import com.palantir.spark.benchmark.queries.QuerySessionIdentifier;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Row;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

public final class BenchmarkMetricsTest extends AbstractLocalSparkTest {
    @Test
    public void testMetrics() {
        String experimentName = "test-experiment-" + UUID.randomUUID().toString();
        BenchmarkPaths paths = new BenchmarkPaths(experimentName);
        BenchmarkMetrics metrics =
                new BenchmarkMetrics(SparkConfiguration.builder().build(), experimentName, paths, sparkSession);
        QuerySessionIdentifier identifier1 = QuerySessionIdentifier.create("q1", 10);
        metrics.startBenchmark(identifier1);
        metrics.stopBenchmark(identifier1);
        metrics.markVerificationFailed(identifier1);

        QuerySessionIdentifier identifier2 = QuerySessionIdentifier.create("q2", 10);
        metrics.startBenchmark(identifier2);
        metrics.stopBenchmark(identifier2);

        // drop sparkConf for legibility on test failures
        List<Row> metricsRows = metrics.getMetricsDataset().drop("sparkConf").collectAsList();
        assertThat(metricsRows).hasSize(2);
        assertThat(metrics.getMetricsDataset().selectExpr("failedVerification").collectAsList().stream()
                        .map(row -> row.getBoolean(0)))
                .containsExactlyInAnyOrder(true, false);
        assertThat(metrics.getMetricsDataset().selectExpr("sessionId").collectAsList().stream()
                        .map(row -> row.getString(0))
                        .map(UUID::fromString)
                        .distinct())
                .hasSize(2);

        metrics.flushMetrics();
        assertThat(metrics.getMetricsDataset().collectAsList()).isEmpty();
        assertThat(sparkSession
                        .read()
                        .schema(BenchmarkMetric.schema())
                        .json(paths.metricsDir())
                        .drop("sparkConf")
                        .collectAsList())
                .containsExactlyInAnyOrderElementsOf(metricsRows);

        // clean up
        Files.delete(Paths.get(paths.metricsDir()).toFile());
    }
}
