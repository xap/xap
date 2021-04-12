package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.QueryColumn;

public class JoinInfo {

    private final QueryColumn leftColumn;
    private final QueryColumn rightColumn;
    private final JoinType joinType;

    public JoinInfo(QueryColumn leftColumn, QueryColumn rightColumn, JoinType joinType) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.joinType = joinType;
    }

    public boolean checkJoinCondition(){
        if(joinType.equals(JoinType.INNER))
            return leftColumn.getCurrentValue().equals(rightColumn.getCurrentValue());
        return true;
    }

    public QueryColumn getLeftColumn() {
        return leftColumn;
    }

    public QueryColumn getRightColumn() {
        return rightColumn;
    }

    public JoinType getJoinType() {
        return joinType;
    }
}
