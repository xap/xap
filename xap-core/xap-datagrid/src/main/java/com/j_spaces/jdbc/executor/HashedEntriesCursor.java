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
import com.j_spaces.jdbc.Join;
import com.j_spaces.jdbc.parser.ColumnNode;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.QueryTableData;
import com.j_spaces.kernel.JSpaceUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * Hash-based simple index for entry packets. Used for equi-joins.
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class HashedEntriesCursor implements EntriesCursor {
    private static final List<IEntryPacket> SINGLE_NULL = Collections.singletonList(null);
    private HashMap<Object, List<IEntryPacket>> hashMap = new HashMap<Object, List<IEntryPacket>>();

    private QueryTableData _joinTable;
    private Iterator<IEntryPacket> _cursor;

    private ColumnNode _joinCol;

    private IEntryPacket _currentEntry;

    private boolean leftJoin;

    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    /**
     * @param table
     * @param indexNode
     */
    public HashedEntriesCursor(QueryTableData table, ExpNode indexNode, IQueryResultSet entries) {
        super();

        ColumnNode leftChild = (ColumnNode) indexNode.getLeftChild();
        ColumnNode rightChild = (ColumnNode) indexNode.getRightChild();

        init(leftChild, rightChild, entries);
        leftJoin = table.getJoinType() == Join.JoinType.LEFT;
    }

    /**
     * @param joinCol
     * @param indexCol
     * @param entries
     */
    private void init(ColumnNode joinCol, ColumnNode indexCol, IQueryResultSet<IEntryPacket> entries) {
        _joinCol = joinCol;
        _joinTable = _joinCol.getColumnData().getColumnTableData();

        for (IEntryPacket entry : entries) {
            Object fieldValue = indexCol.getFieldValue(entry);

            List<IEntryPacket> entriesOnSameIndex = hashMap.get(fieldValue);

            if (entriesOnSameIndex == null) {
                entriesOnSameIndex = new LinkedList<IEntryPacket>();
                hashMap.put(fieldValue, entriesOnSameIndex);
            }

            entriesOnSameIndex.add(entry);
        }
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.EntriesIndex#getMatch(com.j_spaces.jdbc.parser.ColumnNode, com.gigaspaces.internal.transport.IEntryPacket)
     */
    public List<IEntryPacket> getMatch(ColumnNode matchCol,
                                       IEntryPacket expectedMatch) {
        Object fieldValue = matchCol.getFieldValue(expectedMatch);

        return hashMap.get(fieldValue);

    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.EntriesIndex#getCurrentEntry()
     */
    public IEntryPacket getCurrentEntry() {
        _logger.info( "HashedEntriesCursor, hashCode=" + hashCode() + ", getCurrentEntry=" + _currentEntry );
        return _currentEntry;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.EntriesIndex#next()
     */
    public boolean next() {

        _logger.info( "HashedEntriesCursor, START NEXT, hashCode=" + hashCode() + ", _cursor=" + _cursor );

        //check if cursor needs to be initialized
        if (_cursor == null) {
            IEntryPacket joinEntryPacket = _joinTable.getCurrentEntry();
            List<IEntryPacket> match = getMatch(_joinCol, joinEntryPacket);

            if (match == null) {
                if (!leftJoin) {
                    _logger.info( "HashedEntriesCursor, RETURN FALSE 1, hashCode=" + hashCode()  );
                    return false;
                }
                match = SINGLE_NULL;
            }
            _cursor = match.iterator();
        }

        _logger.info( "HashedEntriesCursor, hashCode=" + hashCode() + ", before next(), stacktrace:" + JSpaceUtilities.getStackTrace(new Exception("~~DEBUG~~")));
        if (_cursor.hasNext()) {
            _currentEntry = _cursor.next();
            _logger.info( "HashedEntriesCursor, hashCode=" + hashCode() + ", INIT currentEntry:" + _currentEntry );
            return true;
        }


        _logger.info( "HashedEntriesCursor, RETURN FALSE 2, hashCode=" + hashCode()  );
        return false;

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
