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
package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.TableContainer;

import java.util.LinkedList;
import java.util.List;

public class JoinTablesIterator {
    private final TableContainer startingPoint;

    public JoinTablesIterator(List<TableContainer> tableContainers) {
        startingPoint = findStartingPoint(tableContainers);
    }

    private TableContainer findStartingPoint(List<TableContainer> tableContainers){
        LinkedList<LinkedList<TableContainer>> sequences = new LinkedList<>();
        for(TableContainer tableContainer : tableContainers){
            if(!tableContainer.isJoined()) {
                LinkedList<TableContainer> seq = new LinkedList<>();
                seq.add(tableContainer);
                while(tableContainer.getJoinedTable() != null){
                    tableContainer = tableContainer.getJoinedTable();
                    seq.add(tableContainer);
                }
                sequences.add(seq);
            }
        }
        TableContainer lastJoined = null;
        for (LinkedList<TableContainer> seq: sequences){
            if(lastJoined != null){
                lastJoined.setJoinedTable(seq.getFirst());
                seq.getFirst().setJoined(true);
            }
            lastJoined = seq.getLast();
        }

        for (TableContainer queryResult: tableContainers){
            if(!queryResult.isJoined())
                return queryResult;
        }
        return null;
    }

    public boolean hasNext(){
        if(startingPoint == null)
            return false;
        return startingPoint.getQueryResult().next();
    }

    public TableContainer getStartingPoint() {
        return startingPoint;
    }
}
