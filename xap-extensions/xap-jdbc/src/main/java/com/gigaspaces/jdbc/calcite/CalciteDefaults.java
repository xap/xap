package com.gigaspaces.jdbc.calcite;

import java.util.Properties;

public class CalciteDefaults {

    private static final String DRIVER_KEY = "v3driver";
    private static final String DRIVER_VALUE = "calcite";

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

    public static void setCalciteDriverSystemProperty() {
        System.setProperty(DRIVER_KEY, DRIVER_VALUE);
    }
}
