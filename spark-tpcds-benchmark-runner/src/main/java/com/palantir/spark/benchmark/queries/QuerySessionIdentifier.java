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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.spark.benchmark.immutables.ImmutablesStyle;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesStyle
@JsonSerialize(as = ImmutableQuerySessionIdentifier.class)
@JsonDeserialize(as = ImmutableQuerySessionIdentifier.class)
public interface QuerySessionIdentifier {
    String NO_SESSION = "NO_SESSION";
    String SESSION_ID = UUID.randomUUID().toString();

    @Value.Parameter
    String queryName();

    @Value.Parameter
    int scale();

    @Value.Default
    default String session() {
        return SESSION_ID;
    }

    final class Builder extends ImmutableQuerySessionIdentifier.Builder {}

    static Builder builder() {
        return new Builder();
    }

    static QuerySessionIdentifier createDefault(String queryName, int scale) {
        return builder().queryName(queryName).scale(scale).build();
    }

    static QuerySessionIdentifier createUnique(String queryName, int scale) {
        return builder()
                .queryName(queryName)
                .scale(scale)
                .session(UUID.randomUUID().toString())
                .build();
    }
}
