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

import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public final class DefaultParquetTransformer implements ParquetTransformer {
    @Override
    public void transform(
            SparkSession sparkSession,
            StructType schema,
            Set<String> sourcePaths,
            String destinationPath,
            String delimiter) {
        Optional<Dataset<Row>> unioned = sourcePaths.stream()
                .map(path -> sparkSession
                        .read()
                        .format("csv")
                        .option("delimiter", delimiter)
                        .schema(schema)
                        .load(path))
                .reduce(Dataset::union);
        if (!unioned.isPresent()) {
            return;
        }
        unioned.get().write().format("parquet").save(destinationPath);
    }
}
