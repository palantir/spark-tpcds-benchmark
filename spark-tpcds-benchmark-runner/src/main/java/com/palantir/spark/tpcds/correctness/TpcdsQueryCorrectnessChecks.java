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

package com.palantir.spark.tpcds.correctness;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.palantir.spark.tpcds.paths.TpcdsPaths;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TpcdsQueryCorrectnessChecks {

    private static final Logger log = LoggerFactory.getLogger(TpcdsQueryCorrectnessChecks.class);

    private final TpcdsPaths paths;
    private final FileSystem dataFileSystem;
    private final SparkSession spark;

    public TpcdsQueryCorrectnessChecks(TpcdsPaths paths, FileSystem dataFileSystem, SparkSession spark) {
        this.paths = paths;
        this.dataFileSystem = dataFileSystem;
        this.spark = spark;
    }

    public void verifyCorrectness(
            int scale, String queryName, String sqlStatement, StructType resultSchema, String resultsPath)
            throws IOException {
        spark.sparkContext().setJobDescription(String.format("%s-table-hash-correctness", queryName));
        Dataset<Row> writtenResult =
                spark.read().format("parquet").schema(resultSchema).load(resultsPath);
        byte[] resultHash = writtenResult
                .javaRDD()
                .map(SingleHashFunction.INSTANCE)
                .map(hashCode -> SerializableOptional.of(hashCode))
                .fold(SerializableOptional.empty(), CombineHashFunction.INSTANCE)
                .optional
                .map(HashCode::asBytes)
                .orElse(new byte[] {});
        Path hashCodePath = new Path(paths.experimentCorrectnessHashesLocation(scale, queryName));
        if (dataFileSystem.isFile(hashCodePath)) {
            try (InputStream previousHashCodeInput = dataFileSystem.open(hashCodePath)) {
                byte[] previousHashCodeBytes = ByteStreams.toByteArray(previousHashCodeInput);
                if (!Arrays.equals(resultHash, previousHashCodeBytes)) {
                    throw new ExperimentResultsIncorrectException(String.format(
                            "Experiment results were incorrect.\n"
                                    + "Experiment name: %s\n"
                                    + "Experiment scale: %d\n"
                                    + "Experiment results path: %s\n"
                                    + "Experiment sql:\n\n%s",
                            queryName, scale, resultsPath, sqlStatement));
                }
            }
        } else {
            try (OutputStream currentHashCodeOutput = dataFileSystem.create(hashCodePath, true)) {
                currentHashCodeOutput.write(resultHash);
            }
        }
    }

    private static final class SingleHashFunction implements Function<Row, HashCode>, Serializable {

        static final Function<Row, HashCode> INSTANCE = new SingleHashFunction();

        @Override
        public HashCode call(Row row) {
            return HashCode.fromLong(row.hashCode());
        }
    }

    private static final class CombineHashFunction
            implements Function2<
                            SerializableOptional<HashCode>,
                            SerializableOptional<HashCode>,
                            SerializableOptional<HashCode>>,
                    Serializable {

        static final CombineHashFunction INSTANCE = new CombineHashFunction();

        @Override
        public SerializableOptional<HashCode> call(
                SerializableOptional<HashCode> first, SerializableOptional<HashCode> second) {
            if (first.optional.isPresent() && second.optional.isPresent()) {
                return SerializableOptional.of(Hashing.combineUnordered(() ->
                        Stream.of(first.optional.get(), second.optional.get()).iterator()));
            } else if (!first.optional.isPresent() && !second.optional.isPresent()) {
                return SerializableOptional.empty();
            } else if (first.optional.isPresent()) {
                return first;
            } else {
                return second;
            }
        }
    }

    private static final class ExperimentResultsIncorrectException extends RuntimeException {

        ExperimentResultsIncorrectException(String message) {
            super(message);
        }
    }

    private static final class SerializableOptional<T> implements Serializable {
        private Optional<T> optional;

        private SerializableOptional(T value) {
            this.optional = Optional.ofNullable(value);
        }

        private SerializableOptional(Optional<T> value) {
            this.optional = value;
        }

        static <T> SerializableOptional<T> empty() {
            return of(Optional.empty());
        }

        static <T> SerializableOptional<T> of(T value) {
            return new SerializableOptional<>(value);
        }

        static <T> SerializableOptional<T> of(Optional<T> value) {
            return new SerializableOptional<>(value);
        }

        private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
            boolean isPresent = input.readBoolean();
            if (isPresent) {
                optional = Optional.of((T) input.readObject());
            } else {
                optional = Optional.empty();
            }
        }

        private void writeObject(ObjectOutputStream output) throws IOException {
            if (optional.isPresent()) {
                output.writeBoolean(true);
                output.writeObject(optional.get());
            } else {
                output.writeBoolean(false);
            }
        }
    }
}
