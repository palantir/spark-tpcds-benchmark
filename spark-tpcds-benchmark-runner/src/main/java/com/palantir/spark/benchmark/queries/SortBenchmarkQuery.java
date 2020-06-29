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

package com.palantir.spark.benchmark.queries;

import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public final class SortBenchmarkQuery implements Query {
    public static final String TABLE_NAME = "gensort_data";

    private final Supplier<SparkSession> sparkSessionSupplier;

    public SortBenchmarkQuery(Supplier<SparkSession> sparkSessionSupplier) {
        this.sparkSessionSupplier = sparkSessionSupplier;
    }

    @Override
    public String getName() {
        return "gen-sort-benchmark";
    }

    @Override
    public Optional<String> getSqlStatement() {
        return Optional.empty();
    }

    @Override
    public StructType getSchema() {
        return null;
    }

    @Override
    public void save(String resultLocation) {
        sparkSessionSupplier
                .get()
                .table(TABLE_NAME)
                .sort("record")
                .write()
                .format("parquet")
                .save(resultLocation);
    }
}
