package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.QueryColumn;
import net.sf.jsqlparser.statement.select.Join;

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

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL;

        public static JoinType getType(Join join){
            if(join.isLeft())
                return LEFT;
            if(join.isRight())
                return RIGHT;
            if (join.isOuter() || join.isFull())
                return FULL;
            return INNER;
        }

        public static byte toCode(JoinType joinType) {
            if (joinType == null)
                return 0;
            switch (joinType) {
                case INNER: return 1;
                case LEFT: return 2;
                case RIGHT: return 3;
                case FULL: return 4;
                default: throw new IllegalArgumentException("Unsupported join type: " + joinType);
            }
        }

        public static JoinType fromCode(byte code) {
            switch (code) {
                case 0: return null;
                case 1: return INNER;
                case 2: return LEFT;
                case 3: return RIGHT;
                case 4: return FULL;
                default: throw new IllegalArgumentException("Unsupported join code: " + code);
            }
        }
    }

    public enum JoinAlgorithm {
        Nested, Hash, SortMerge
    }
}
