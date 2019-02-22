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

package com.palantir.spark.tpcds.registration;

import com.palantir.spark.tpcds.constants.TpcdsTable;
import com.palantir.spark.tpcds.paths.TpcdsPaths;
import com.palantir.spark.tpcds.schemas.TpcdsSchemas;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

public final class TpcdsTableRegistration {

    private final TpcdsPaths paths;
    private final FileSystem dataFileSystem;
    private final SparkSession spark;
    private final TpcdsSchemas schemas;

    public TpcdsTableRegistration(
            TpcdsPaths paths,
            FileSystem dataFileSystem,
            SparkSession spark,
            TpcdsSchemas schemas) {
        this.paths = paths;
        this.dataFileSystem = dataFileSystem;
        this.spark = spark;
        this.schemas = schemas;
    }

    public void registerTables(int scale) {
        Stream.of(TpcdsTable.values()).forEach(table -> {
            try {
                String tableLocation = paths.tableParquetLocation(scale, table);
                Path tablePath = new Path(tableLocation);
                if (!dataFileSystem.isDirectory(tablePath)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Table %s not found in Parquet format at %s; was the data generated accordingly?",
                                    table,
                                    tableLocation));
                }
                spark.read()
                        .format("parquet")
                        .schema(schemas.getSchema(table))
                        .load(paths.tableParquetLocation(scale, table))
                        .createOrReplaceTempView(table.tableName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
