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

import java.text.SimpleDateFormat;

/**
 * Returns current date formatted
 *
 * @author Evgeny Fisher
 * @since 16.0.0
 */
@com.gigaspaces.api.InternalApi
public class DayOfYearSqlFunction extends AbstractDateRelatedSqlFunction {

    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("D");

    /**
     * @param context which contains one argument of type string.
     * @return Returns the string context.getArgument(0) with all characters changed to uppercase.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        return simpleDateFormat.format( verifyArgumentsAndGetDate( "DayOfYear", context ) );
    }

    public static void main( String[] args ){

        SqlFunctionExecutionContext context = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return index == 0 ? "2021-05-20" : null;
            }
        };

        System.out.println( ( new DayOfYearSqlFunction() ).apply(context) );
    }
}