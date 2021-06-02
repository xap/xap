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

import com.gigaspaces.logger.Constants;

import java.sql.SQLException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public abstract class AbstractDateTimeParser extends AbstractParser {
    protected final String _desc;
    protected final String _pattern;

    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    protected AbstractDateTimeParser(String desc, String pattern) {
        this._desc = desc;
        this._pattern = pattern;
        if( _logger.isDebugEnabled() ) {
            _logger.debug("> AbstractDateTimeParser desc=" + desc + ", pattern=" + _pattern);
        }
    }

    protected java.util.Date parseDateTime(String s) throws SQLException {
        // NOTE: SimpleDateFormat is not thread-safe
        final SimpleDateFormat format = new SimpleDateFormat(_pattern);
        final java.util.Date date = format.parse(s, new ParsePosition(0));
        if (date == null)
            throw new SQLException("Wrong " + _desc + " format, expected format=[" + _pattern + "], provided=[" + s + "]", "GSP", -378);
        return date;
    }
}
