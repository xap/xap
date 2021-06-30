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
import java.util.Date;

/**
 * Returns current time formatted
 *
 * @author Evgeny Fisher
 * @since 16.0.0
 */
@com.gigaspaces.api.InternalApi
public class CurrentTimeSqlFunction extends SqlFunction {

    private final String pattern = "hh:mm:ss.mmm";
    private final SimpleDateFormat simpleTimeFormat = new SimpleDateFormat(pattern);

    /**
     * @param context which contains one argument of type string.
     * @return Returns the string context.getArgument(0) with all characters changed to uppercase.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {

        return simpleTimeFormat.format( new Date() );
    }

    public static void main( String[] args ){
        System.out.println( ( new CurrentTimeSqlFunction() ).apply(null) );
    }
}
