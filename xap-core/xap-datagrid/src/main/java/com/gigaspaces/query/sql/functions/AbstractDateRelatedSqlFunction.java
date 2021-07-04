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

import com.j_spaces.jdbc.QueryProcessor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Abstract date and time related functions class
 *
 * @author Evgeny Fisher
 * @since 16.0.0
 */

abstract class AbstractDateRelatedSqlFunction extends SqlFunction {

    private final String pattern = QueryProcessor.getDefaultConfig().getLocalDateTimeFormat();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat( pattern );
    protected final Calendar calendar = Calendar.getInstance();

    protected Date verifyArgumentsAndGetDate( String functionName, SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, context);
        Object arg = context.getArgument(0);
        if (!(arg instanceof String)) {// fast fail
            throw new RuntimeException( functionName + " function - wrong argument type, should be a String - : " + arg);
        }
        Date date;
        try {
            date = dateFormat.parse( ( String )arg );
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse: " + arg + " using pattern [" + pattern + "]");
        }
        return date;
    }
}