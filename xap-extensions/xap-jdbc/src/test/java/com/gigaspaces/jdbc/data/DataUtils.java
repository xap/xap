/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package com.gigaspaces.jdbc.data;

import java.util.List;
import java.util.stream.Collectors;

public class DataUtils {

    public static String modifyEmployeeForHsqlQuery(String query) {
        return query.replace("com.gigaspaces.test.database.jdbc.v3driver.data.", "");
    }

    public static String modifyClassForGigaQuery(String hsqlQuery, String tableName, String className ) {
        return hsqlQuery.replace(tableName, className);
    }

    public static List<String> modifyEmployeeForExplainPlan(List<String> explainPlan, String tableName, String className) {
        return explainPlan.stream().map(row -> row.replace(tableName, className)).collect(Collectors.toList());
    }

}
