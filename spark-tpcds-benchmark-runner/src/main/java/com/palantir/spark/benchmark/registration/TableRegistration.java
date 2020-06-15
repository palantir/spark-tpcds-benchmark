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

package com.palantir.spark.benchmark.registration;

import com.palantir.spark.benchmark.constants.TpcdsTable;
import com.palantir.spark.benchmark.paths.BenchmarkPaths;
import com.palantir.spark.benchmark.queries.SortBenchmarkQuery;
import com.palantir.spark.benchmark.schemas.Schemas;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public final class TableRegistration {
    private final BenchmarkPaths paths;
    private final FileSystem dataFileSystem;
    private final SparkSession spark;
    private final Schemas schemas;

    public TableRegistration(BenchmarkPaths paths, FileSystem dataFileSystem, SparkSession spark, Schemas schemas) {
        this.paths = paths;
        this.dataFileSystem = dataFileSystem;
        this.spark = spark;
        this.schemas = schemas;
    }

    public void registerTables(int scale) {
        Stream.of(TpcdsTable.values()).forEach(table -> {
            registerTable(table.tableName(), schemas.getSchema(table), scale);
        });
    }

    public void registerGensortTable(int scale) {
        registerTable(SortBenchmarkQuery.TABLE_NAME, schemas.getGensortSchema(), scale);
    }

    private void registerTable(String tableName, StructType schema, int scale) {
        String tableLocation = paths.tableParquetLocation(scale, tableName);
        Path tablePath = new Path(tableLocation);
        try {
            if (!dataFileSystem.isDirectory(tablePath)) {
                throw new IllegalArgumentException(String.format(
                        "Table %s not found in Parquet format at %s; was the data generated accordingly?",
                        tableName, tableLocation));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        spark.read().format("parquet").schema(schema).load(tableLocation).createOrReplaceTempView(tableName);
    }
}
