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

package com.palantir.spark.tpcds.datagen;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public final class DefaultParquetCopier implements ParquetCopier {
    @Override
    public void copy(SparkSession sparkSession, StructType schema, String sourcePath, String destinationPath) {
        Dataset<Row> tableDataset = sparkSession
                .read()
                .format("csv")
                .option("delimiter", "|")
                .schema(schema)
                .load(sourcePath);
        tableDataset.write().format("parquet").save(destinationPath);
    }
}
