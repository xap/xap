package com.gigaspaces.jdbc.calcite;

import java.util.Properties;

public class CalciteDefaults {


    public static final String DRIVER_KEY = "com.gs.jdbc.v3.driver";
    public static final String DRIVER_DEFAULT = "jsql";
    public static final String DRIVER_VALUE = "calcite";

    public static final String SUPPORT_INEQUALITY = "com.gs.jdbc.v3.support.inequality";
    public static final String SUPPORT_SEMICOLON_SEPARATOR = "com.gs.jdbc.v3.semicolon_separator";


    public static boolean isCalciteDriverPropertySet() {
        return DRIVER_VALUE.equals(System.getProperty(DRIVER_KEY));
    }

    public static boolean isCalciteDriverPropertySet(Properties properties) {
        if (isCalciteDriverPropertySet()) return true;
        if (properties != null) {
            return DRIVER_VALUE.equals(properties.getProperty(DRIVER_KEY));
        }
        return false;
    }

    public static boolean isCalcitePropertySet(String key, Properties properties) {
        String value = System.getProperty(key);
        if (value == null && properties != null) {
            value = properties.getProperty(key);
        }

        return "true".equals(value);
    }

    public static void setCalciteDriverSystemProperty() {
        System.setProperty(DRIVER_KEY, DRIVER_VALUE);
    }
}
