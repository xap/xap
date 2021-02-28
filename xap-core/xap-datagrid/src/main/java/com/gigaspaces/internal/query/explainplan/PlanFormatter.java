package com.gigaspaces.internal.query.explainplan;

import java.util.Map;

public class PlanFormatter {
    public static String format(Map<String, Object> plan) {
        final TextReportFormatter textReportFormatter = new TextReportFormatter();
        for (Map.Entry<String, Object> entry : plan.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

        }
        return null;
    }
}
