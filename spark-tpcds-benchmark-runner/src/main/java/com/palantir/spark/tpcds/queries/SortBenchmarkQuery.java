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
import com.palantir.spark.tpcds.constants.TpcdsTable;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public final class SortBenchmarkQuery implements Query {
    private final SparkSession spark;
    private final String resultLocation;
    private final Supplier<Dataset<Row>> datasetSupplier;

    public SortBenchmarkQuery(SparkSession spark, String resultLocation) {
        this.spark = spark;
        this.resultLocation = resultLocation;
        this.datasetSupplier = Suppliers.memoize(this::buildDataset);
    }

    public String getName() {
        return "SparkSqlSortBenchmark";
    }

    @Override
    public Optional<String> getSqlStatement() {
        return Optional.empty();
    }

    @Override
    public boolean hasCorrectnessCheck() {
        return false;
    }

    @Override
    public StructType getSchema() {
        return null;
    }

    @Override
    public void save() {
        this.datasetSupplier.get()
                .write()
                .bucketBy(2500, "datahashbucket")
                .sortBy("datahashbucket")
                .format("parquet")
                .save(resultLocation);
    }

    private Dataset<Row> buildDataset() {
        Dataset<Row> input = spark.table(TpcdsTable.STORE_SALES.tableName());
        ExpressionEncoder<Row> encoder = RowEncoder.apply(input.schema());
        return input
                .withColumn("col1", functions.hash(functions.column("ss_customer_sk")))
                .sort("col1")
                .mapPartitions(new ForceFailureFunction<>(0), encoder)
                .repartition(400)
                .repartition(1400)
                .withColumn("col2", functions.pmod(functions.col("col1"), functions.lit(100)))
                .sort("col2")
                .mapPartitions(new ForceFailureFunction<>(1), encoder)
                .repartition(1500)
                .withColumn("col3", functions.pmod(functions.col("col1"), functions.factorial(functions.col("col2"))))
                .sort("col3")
                .withColumn("col1hash", functions.hash(functions.column("col1")))
                .withColumn("hashbucket", functions.pmod(functions.column("col1hash"), functions.lit(2500)))
                .withColumn("col2long", functions.column("col2").cast(DataTypes.LongType))
                .groupBy("hashbucket")
                .agg(functions.sum("part2long").as("part2sum"))
                .sort("hashbucket");
    }

    static final class ForceFailureFunction<T> implements MapPartitionsFunction<T, T> {
        private final int failurePartitionId;

        ForceFailureFunction(int failurePartitionId) {
            this.failurePartitionId = failurePartitionId;
        }

        public Iterator<T> call(Iterator<T> input) {
            if (TaskContext.get().stageAttemptNumber() == 0 && TaskContext.get().partitionId() == failurePartitionId) {
                throw new RuntimeException("intentional failure");
            }
            return input;
        }
    }
}
