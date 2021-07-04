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

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 *
 * @author Evgeny Fisher
 * @since 16.0.0
 */
@com.gigaspaces.api.InternalApi
public class TruncSqlFunction extends SqlFunction {

    /**
     * Context can have from 1 to 2 arguments, the first is number ( can be float ), the second one is precision, it is optional,
     * whole number.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, 2, context);
        Object number = context.getArgument(0);
        int numberOfArguments = context.getNumberOfArguments();
        if (!(number instanceof Number)) {// fast fail
            throw new RuntimeException("Trunc function - wrong argument type, should be a Number - : " + number);
        }

        if( numberOfArguments == 1 ){
            NumberFormat decimalFormat = getDecimalFormatter((Number) number, 0);
            return decimalFormat.format( number );
        }
        else{
            Object precisionObj = context.getArgument(1);
            if (!(precisionObj instanceof Integer) && !(precisionObj instanceof Long) && !(precisionObj instanceof Short) ) {// fast fail
                throw new RuntimeException("Trunc function - precision argument must be integer - : " + precisionObj);
            }
            int precision = ( ( Number )precisionObj ).intValue();
            //if precision is positive
            if( precision >= 0 ){
                NumberFormat decimalFormat = getDecimalFormatter((Number) number, precision);
                return decimalFormat.format( number );
            }
            //if precision is negative
            else{
                NumberFormat decimalFormat = getDecimalFormatter((Number) number, 0);
                String formattedNumber = decimalFormat.format(number);
                int precisionAbs = Math.abs(precision);
                if( precisionAbs >= formattedNumber.length() ){
                    return String.valueOf(0);
                }
                else{
                    String subStr = formattedNumber.substring(0, formattedNumber.length() - precisionAbs );
                    StringBuilder stringBuilder = new StringBuilder( subStr );
                    for( int i = 0; i < precisionAbs; i++){
                        stringBuilder.append( 0 );
                    }
                    return stringBuilder.toString();
                }
            }
        }
    }

    public NumberFormat getDecimalFormatter(Number number, int precision) {
        DecimalFormat decimalFormat = new DecimalFormat("##");
        decimalFormat.setRoundingMode(number.doubleValue() >= 0 ? RoundingMode.FLOOR : RoundingMode.CEILING);
        decimalFormat.setMaximumFractionDigits( precision );
        decimalFormat.setMinimumFractionDigits( precision );
        return decimalFormat;
    }
}