/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.spark.tpcds.queries;

import com.google.common.base.Suppliers;
import com.palantir.spark.tpcds.constants.TpcdsTable;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public final class SortBenchmarkQuery implements Query {
    private final SparkSession spark;
    private final Supplier<Dataset<Row>> datasetSupplier;

    public SortBenchmarkQuery(SparkSession spark) {
        this.spark = spark;
        this.datasetSupplier = Suppliers.memoize(this::buildDataset);
    }

    @Override
    public String getName() {
        return "pal-sort-benchmark";
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
        this.datasetSupplier
                .get()
                .write()
                // .bucketBy(2500, "hashbucket")
                // .sortBy("hashbucket")
                .format("parquet")
                .save(resultLocation);
    }

    private Dataset<Row> buildDataset() {
        // ExpressionEncoder<Row> encoder = RowEncoder.apply(input.schema());
        Dataset<Row> withHashCol1 = spark.table(TpcdsTable.STORE_SALES.tableName())
                .withColumn("col1", functions.hash(functions.col("ss_customer_sk")))
                .sort("col1");
        Dataset<Row> withHashCol2 = withHashCol1
                .mapPartitions(new NoOpMapPartitionsFunction<>(), RowEncoder.apply(withHashCol1.schema()))
                .repartition(400)
                .mapPartitions(new NoOpMapPartitionsFunction<>(), RowEncoder.apply(withHashCol1.schema()))
                .repartition(700)
                .withColumn("col2", functions.pmod(functions.col("col1"), functions.lit(100)))
                .sort("col2");
        return withHashCol2
                .mapPartitions(new NoOpMapPartitionsFunction<>(), RowEncoder.apply(withHashCol2.schema()))
                .repartition(500)
                .withColumn("col3", functions.pmod(functions.col("col1"), functions.factorial(functions.col("col2"))))
                .sort("col3")
                .withColumn("col1hash", functions.hash(functions.column("col1")))
                .withColumn("hashbucket", functions.pmod(functions.column("col1hash"), functions.lit(2500)))
                .withColumn("col2long", functions.column("col2").cast(DataTypes.LongType))
                .groupBy("hashbucket")
                .agg(functions.sum("col2long").as("col2sum"))
                .sort("hashbucket");
    }

    static final class NoOpMapPartitionsFunction<T> implements MapPartitionsFunction<T, T> {
        NoOpMapPartitionsFunction() {}

        @Override
        public Iterator<T> call(Iterator<T> input) {
            return input;
        }
    }
}
