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
