package com.gigaspaces.jdbc.calcite.experimental.result;


import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.join.JoinInfo;


import java.util.*;

public class HashedRowCursor implements Cursor<TableRow> {
    private static final List<TableRow> SINGLE_NULL = Collections.singletonList(null);
    private final Map<Object, List<TableRow>> hashMap = new HashMap<>();
    private final JoinInfo joinInfo;
    private Iterator<TableRow> iterator;
    private TableRow current;

    public HashedRowCursor(JoinInfo joinInfo, List<TableRow> rows) {
        this.joinInfo = joinInfo;
        init(rows);
    }

    private void init(List<TableRow> rows) {
        String joinColumn = joinInfo.getRightColumn();
        for (TableRow row : rows) {
            List<TableRow> rowsWithSameIndex = hashMap.computeIfAbsent(row.getPropertyValue(joinColumn), k -> new LinkedList<>());
            rowsWithSameIndex.add(row);
        }
    }

    @Override
    public boolean next() {
        if(iterator == null){
            List<TableRow> match = hashMap.get(getCurrent().getPropertyValue(joinInfo.getLeftColumn()));
            if(match == null) {
                if(!joinInfo.getJoinType().equals(JoinInfo.JoinType.LEFT))
                    return false;
                match = SINGLE_NULL;
            }
            iterator = match.iterator();
        }
        if(iterator.hasNext()){
            current = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public TableRow getCurrent() {
        return current;
    }

    @Override
    public void reset() {
        iterator = null;
    }

    @Override
    public boolean isBeforeFirst() {
        return iterator == null;
    }
}
