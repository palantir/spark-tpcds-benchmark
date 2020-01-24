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

package com.palantir.spark.tpcds.queries;

import com.google.common.base.Suppliers;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public final class SqlQuery implements Query {
    private final String queryName;
    private final String query;
    private final String resultLocation;
    private final Supplier<Dataset<Row>> datasetSupplier;

    public SqlQuery(SparkSession spark, String queryName, String query, String resultLocation) {
        this.queryName = queryName;
        this.query = query;
        this.resultLocation = resultLocation;
        this.datasetSupplier = Suppliers.memoize(() -> sanitizeColumnNames(spark.sql(query)));
    }

    @Override
    public String getName() {
        return queryName;
    }

    @Override
    public Optional<String> getSqlStatement() {
        return Optional.of(query);
    }

    @Override
    public StructType getSchema() {
        return datasetSupplier.get().schema();
    }

    @Override
    public void save() {
        datasetSupplier.get().write().format("parquet").save(resultLocation);
    }

    private Dataset<Row> sanitizeColumnNames(Dataset<Row> sqlOutput) {
        Dataset<Row> sanitizedSqlOutput = sqlOutput;
        for (String columnName : sqlOutput.columns()) {
            String sanitized = sanitize(columnName);
            if (!sanitized.equals(columnName)) {
                sanitizedSqlOutput = sanitizedSqlOutput.withColumnRenamed(columnName, sanitized);
            }
        }
        return sanitizedSqlOutput;
    }

    private static String sanitize(String name) {
        return name.replaceAll("[ ,;{}()]", "X");
    }
}
