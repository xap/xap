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

/**
 *
 */
package com.j_spaces.jdbc.executor;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.jdbc.query.IQueryResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;


/**
 * No index - just scan all the entries and find matches
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ScanCursor
        implements EntriesCursor {
    private IQueryResultSet<IEntryPacket> _entries;

    private Iterator<IEntryPacket> _cursor;
    private IEntryPacket _currentEntry;

    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    /**
     * @param tableEntries
     */
    public ScanCursor(IQueryResultSet<IEntryPacket> tableEntries) {
        super();
        _entries = tableEntries;
    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.EntriesIndex#next()
     */
    public boolean next() {
        _logger.info( "ScanCursor, hashCode=" + hashCode() + " START NEXT" );
        //check if cursor needs to be initialized
        if (_cursor == null) {
            _cursor = _entries.iterator();
        }

        if (_cursor.hasNext()) {
            _currentEntry = _cursor.next();
            return true;
        }

        return false;

    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.EntriesIndex#getCurrentEntry()
     */
    public IEntryPacket getCurrentEntry() {
        return _currentEntry;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.EntriesIndex#reset()
     */
    public void reset() {
        _cursor = null;

    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.EntriesCursor#isBeforeFirst()
     */
    public boolean isBeforeFirst() {
        return _cursor == null;
    }


}
