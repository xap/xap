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
