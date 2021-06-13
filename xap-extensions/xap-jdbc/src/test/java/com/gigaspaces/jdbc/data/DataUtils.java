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
