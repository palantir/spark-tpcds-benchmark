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

package com.palantir.spark.benchmark.metrics;

import com.google.common.collect.ImmutableList;
import com.palantir.spark.benchmark.immutables.ImmutablesStyle;
import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.immutables.value.Value;
import scala.collection.JavaConverters;

@Value.Immutable
@ImmutablesStyle
public abstract class BenchmarkMetric implements Serializable {

    public static final StructType SPARK_SCHEMA = schema();

    public abstract String experimentName();

    public abstract String queryName();

    public abstract int scale();

    public abstract String sparkVersion();

    public abstract int executorInstances();

    public abstract long executorMemoryMb();

    public abstract int executorCores();

    public abstract String applicationId();

    public abstract long durationMillis();

    public abstract Map<String, String> sparkConf();

    public abstract long experimentStartTimestampMillis();

    public abstract long experimentEndTimestampMillis();

    public static StructType schema() {
        return new StructType(Stream.of(
                        new StructField("experimentName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("queryName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("scale", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("sparkVersion", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("executorInstances", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("executorMemoryMb", DataTypes.LongType, false, Metadata.empty()),
                        new StructField("executorCores", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("applicationId", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("durationMillis", DataTypes.LongType, false, Metadata.empty()),
                        new StructField(
                                "sparkConf",
                                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                                true,
                                Metadata.empty()),
                        new StructField("experimentStartTimestamp", DataTypes.TimestampType, false, Metadata.empty()),
                        new StructField("experimentEndTimestamp", DataTypes.TimestampType, false, Metadata.empty()))
                .toArray(StructField[]::new));
    }

    public final Row toRow() {
        return Row$.MODULE$.apply(JavaConverters.asScalaBufferConverter(ImmutableList.of(
                        queryName(),
                        scale(),
                        sparkVersion(),
                        executorInstances(),
                        executorMemoryMb(),
                        executorCores(),
                        applicationId(),
                        durationMillis(),
                        JavaConverters.mapAsScalaMapConverter(sparkConf()).asScala(),
                        new java.sql.Timestamp(experimentStartTimestampMillis()),
                        new java.sql.Timestamp(experimentEndTimestampMillis())))
                .asScala());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends ImmutableBenchmarkMetric.Builder {}
}
