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

package com.gigaspaces.internal.utils.parsers;

import com.j_spaces.jdbc.QueryProcessor;

import java.sql.SQLException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * @author Kobi
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class LocalTimeParser extends AbstractDateTimeParser {
    private final DateTimeFormatter formatter;

    public LocalTimeParser() {
        super("java.time.LocalTime", QueryProcessor.getDefaultConfig().getLocalTimeFormat());
        this.formatter = DateTimeFormatter.ofPattern(_pattern);
    }

    @Override
    public Object parse(String s) throws SQLException {
        LocalTime date = LocalTime.parse(s, formatter);
        if (date == null)
            throw new SQLException("Wrong " + _desc + " format, expected format=[" + _pattern + "], provided=[" + s + "]", "GSP", -378);

        return date;
    }
}
