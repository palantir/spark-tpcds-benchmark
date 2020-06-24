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

package com.palantir.spark.benchmark.metrics;

import com.google.common.collect.ImmutableList;
import com.palantir.spark.benchmark.immutables.ImmutablesStyle;
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
public interface VerificationStatus {
    String experimentName();

    String queryName();

    int scale();

    boolean failedVerification();

    static StructType schema() {
        return new StructType(Stream.of(
                        new StructField("experimentName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("queryName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("scale", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("failedVerification", DataTypes.BooleanType, false, Metadata.empty()))
                .toArray(StructField[]::new));
    }

    @Value.Lazy
    default Row toRow() {
        return Row$.MODULE$.apply(JavaConverters.<Object>asScalaBufferConverter(
                        ImmutableList.of(experimentName(), queryName(), scale(), failedVerification()))
                .asScala());
    }

    final class Builder extends ImmutableVerificationStatus.Builder {}

    static Builder builder() {
        return new Builder();
    }
}
