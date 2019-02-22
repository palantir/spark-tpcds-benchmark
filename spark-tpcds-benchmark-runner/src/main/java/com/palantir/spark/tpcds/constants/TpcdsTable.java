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

package com.palantir.spark.tpcds.constants;

import java.util.stream.Stream;

public enum TpcdsTable {
    CALL_CENTER("call_center"),
    CATALOG_PAGE("catalog_page"),
    CATALOG_SALES("catalog_sales"),
    CATALOG_RETURNS("catalog_returns"),
    CUSTOMER("customer"),
    CUSTOMER_ADDRESS("customer_address"),
    CUSTOMER_DEMOGRAPHICS("customer_demographics"),
    DATE_DIM("date_dim"),
    HOUSEHOLD_DEMOGRAPHICS("household_demographics"),
    INCOME_BAND("income_band"),
    INVENTORY("inventory"),
    ITEM("item"),
    PROMOTION("promotion"),
    REASON("reason"),
    SHIP_MODE("ship_mode"),
    STORE("store"),
    STORE_RETURNS("store_returns"),
    STORE_SALES("store_sales"),
    TIME_DIM("time_dim"),
    WAREHOUSE("warehouse"),
    WEB_PAGE("web_page"),
    WEB_RETURNS("web_returns"),
    WEB_SALES("web_sales"),
    WEB_SITE("web_site");

    private final String tableName;

    TpcdsTable(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return tableName;
    }

    public String tableName() {
        return tableName;
    }

    public static TpcdsTable of(String stringValue) {
        return Stream.of(TpcdsTable.values())
                .filter(table -> table.tableName().equalsIgnoreCase(stringValue))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException(
                                String.format("No table named %s", stringValue)));
    }
}
