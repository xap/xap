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

package com.gigaspaces.query.sql.functions;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * Built in conversion sql function to convert string type to Number.
 *
 * @author Tamir Schwarz
 * @since 11.0.0
 */
@com.gigaspaces.api.InternalApi
public class ToNumberSqlFunction extends SqlFunction {
    public static final DecimalFormatSymbols DECIMAL_FORMAT_SYMBOLS = DecimalFormatSymbols.getInstance(Locale.ENGLISH);

    /**
     * @param context which contains an argument of type string and can have an additional format
     *                argument.
     * @return Number with corresponding value to context.getArgument(0), in format
     * context.getArgument(1) if exists.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        Object arg = context.getArgument(0);
        Object format = null;
        if (context.getNumberOfArguments() >= 2) {
            format = context.getArgument(1);
        }
        if (arg instanceof String && isNumeric(String.valueOf(arg))) {
            String str = String.valueOf(arg);
            if (format != null) { // with specific format
                if (str.contains(".")) { //double
                    DecimalFormat formatter = new DecimalFormat(String.valueOf(format), DECIMAL_FORMAT_SYMBOLS);
                    double number = Double.parseDouble(String.valueOf(arg));
                    String numberFormatted = formatter.format(number);
                    return Double.parseDouble(numberFormatted);
                } else { // int
                    DecimalFormat formatter = new DecimalFormat(String.valueOf(format), DECIMAL_FORMAT_SYMBOLS);
                    int number = Integer.parseInt(String.valueOf(arg));
                    String numberFormatted = formatter.format(number);
                    return Integer.parseInt(numberFormatted);
                }
            } else {
                if (str.contains(".")) { // double
                    return Double.parseDouble(str);
                } else { // int
                    return Integer.parseInt(str);
                }
            }
        }
        throw new RuntimeException("To_Number function - wrong argument type: " + arg);
    }

    private static boolean isNumeric(String str) {
        return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
    }
}
