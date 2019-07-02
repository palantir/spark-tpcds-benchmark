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

package com.palantir.spark.tpcds.schemas;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.palantir.spark.tpcds.constants.TpcdsTable;
import com.palantir.spark.tpcds.datagen.TpcdsDataGenerator;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class TpcdsSchemas {
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+),(\\d+)\\)");

    private final Map<TpcdsTable, StructType> schemas = Maps.newConcurrentMap();
    private final Supplier<String> cachedSqlSchemaDefinition =
            Suppliers.memoize(TpcdsSchemas::getSqlSchemaDefinition);

    public StructType getSchema(TpcdsTable table) {
        return schemas.computeIfAbsent(table, this::doGetSchema);
    }

    private StructType doGetSchema(TpcdsTable table) {
        String sqlSchemaDefinition = cachedSqlSchemaDefinition.get();

        Pattern pattern = Pattern.compile(String.format("create table %s\\n\\((.*?)\\);", table), Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sqlSchemaDefinition);
        com.palantir.logsafe.Preconditions.checkArgument(
                matcher.find(),
                "SQL schema definition is ill-formatted");
        String group = matcher.group(1);

        List<String> lines = Splitter.on('\n').splitToList(group);
        List<StructField> structFields = lines.stream()
                .filter(line -> !line.contains("primary key"))
                .filter(line -> !line.isEmpty())
                .map(line -> Splitter.on(CharMatcher.whitespace()).omitEmptyStrings().splitToList(line))
                .map(groups -> DataTypes.createStructField(groups.get(0), toSparkType(groups.get(1)), true))
                .collect(Collectors.toList());

        return DataTypes.createStructType(structFields);
    }

    private static DataType toSparkType(String sqlType) {
        if (sqlType.equals("integer")) {
            return DataTypes.IntegerType;
        } else if (sqlType.equals("date")) {
            return DataTypes.DateType;
        } else if (sqlType.equals("time")) {
            return DataTypes.TimestampType;
        } else if (sqlType.contains("char")) {
            return DataTypes.StringType;
        }

        Matcher matcher = DECIMAL_PATTERN.matcher(sqlType);
        if (matcher.find()) {
            return DataTypes.createDecimalType(Integer.valueOf(matcher.group(1)), Integer.valueOf(matcher.group(2)));
        }
        throw new RuntimeException("Unknown sqlType: " + sqlType);
    }

    private static String getSqlSchemaDefinition() {
        try (InputStream schemaSqlDefinition =
                TpcdsDataGenerator.class.getClassLoader().getResourceAsStream("tpcds.sql");
                InputStreamReader schemaSqlReader = new InputStreamReader(
                        schemaSqlDefinition, StandardCharsets.UTF_8)) {
            return CharStreams.toString(schemaSqlReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
