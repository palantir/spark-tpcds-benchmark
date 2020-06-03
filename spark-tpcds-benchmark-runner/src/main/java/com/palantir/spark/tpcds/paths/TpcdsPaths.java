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

package com.palantir.spark.tpcds.paths;

import com.palantir.spark.tpcds.constants.TpcdsTable;
import java.io.File;
import org.apache.commons.lang3.StringUtils;

public final class TpcdsPaths {

    private final String testDataDir;

    public TpcdsPaths(String testDataDir) {
        this.testDataDir = testDataDir;
    }

    public String rootTpcdsDataDir(int scale) {
        return String.join(File.separator, testDataDir, "tpcds_data", String.format("scale=%d", scale));
    }

    public String rootGensortDataDir() {
        return String.join(File.separator, testDataDir, "gensort_data");
    }

    public String tableCsvFile(int scale, TpcdsTable table) {
        return String.join(File.separator, tpcdsCsvDir(scale), String.format("%s.csv", table));
    }

    public String tableParquetLocation(int scale, TpcdsTable table) {
        return String.join(File.separator, tpcdsParquetDir(scale), table.tableName());
    }

    public String tpcdsCsvDir(int scale) {
        return String.join(File.separator, rootTpcdsDataDir(scale), "raw_csv");
    }

    public String tpcdsParquetDir(int scale) {
        return String.join(File.separator, rootTpcdsDataDir(scale), "raw_parquet");
    }

    public String gensortCsvDir() {
        return String.join(File.separator, rootGensortDataDir(), "raw_csv");
    }

    public String experimentResultLocation(int scale, String queryName) {
        return String.join(
                File.separator, rootTpcdsDataDir(scale), "experiments", StringUtils.removeEnd(queryName, ".sql"));
    }

    public String experimentCorrectnessHashesRoot(int scale) {
        return String.join(File.separator, rootTpcdsDataDir(scale), "experiments_correctness_hashes");
    }

    public String experimentCorrectnessHashesLocation(int scale, String queryName) {
        return String.join(
                File.separator, experimentCorrectnessHashesRoot(scale), StringUtils.removeEnd(queryName, ".sql"));
    }

    public String metricsDir() {
        return String.join(File.separator, testDataDir, "tpcds_benchmark_results");
    }
}
