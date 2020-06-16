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

package com.palantir.spark.benchmark.paths;

import com.palantir.spark.benchmark.constants.TpcdsTable;
import java.io.File;
import org.apache.commons.lang3.StringUtils;

public final class BenchmarkPaths {
    private final String experimentName;

    public BenchmarkPaths(String experimentName) {
        this.experimentName = experimentName;
    }

    public String rootDataDir(int scale) {
        return String.join(File.separator, "benchmark_data", String.format("scale=%d", scale));
    }

    public String tableCsvFile(int scale, TpcdsTable table) {
        return tableCsvFile(scale, table.tableName());
    }

    public String tableCsvFile(int scale, String tableName) {
        return String.join(File.separator, csvDir(scale), String.format("%s.csv", tableName));
    }

    public String tableCsvFile(int scale, String tableName, long partitionIndex) {
        return String.join(File.separator, csvDir(scale), String.format("%s_%s.csv", tableName, partitionIndex));
    }

    public String tableParquetLocation(int scale, TpcdsTable table) {
        return tableParquetLocation(scale, table.tableName());
    }

    public String tableParquetLocation(int scale, String tableName) {
        return String.join(File.separator, parquetDir(scale), tableName);
    }

    public String csvDir(int scale) {
        return String.join(File.separator, rootDataDir(scale), "raw_csv");
    }

    public String parquetDir(int scale) {
        return String.join(File.separator, rootDataDir(scale), "raw_parquet");
    }

    public String experimentResultLocation(int scale, String queryName) {
        return String.join(
                File.separator,
                "experiments",
                experimentName,
                String.format("scale=%d", scale),
                StringUtils.removeEnd(queryName, ".sql"));
    }

    public String experimentCorrectnessHashesRoot(int scale) {
        return String.join(
                File.separator, "experiments_correctness_hashes", experimentName, String.format("scale=%d", scale));
    }

    public String experimentCorrectnessHashesLocation(int scale, String queryName) {
        return String.join(
                File.separator, experimentCorrectnessHashesRoot(scale), StringUtils.removeEnd(queryName, ".sql"));
    }

    public String metricsDir() {
        return String.join(File.separator, experimentName, "benchmark_results");
    }
}
