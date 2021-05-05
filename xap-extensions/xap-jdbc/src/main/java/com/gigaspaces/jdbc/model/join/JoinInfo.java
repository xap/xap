package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.statement.select.Join;

public class JoinInfo {

    private final QueryColumn leftColumn;
    private final QueryColumn rightColumn;
    private final JoinType joinType;
    private Range range;

    public JoinInfo(QueryColumn leftColumn, QueryColumn rightColumn, JoinType joinType) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.joinType = joinType;
    }

    public boolean checkJoinCondition(){
        if(joinType.equals(JoinType.INNER))
            return leftColumn.getCurrentValue().equals(rightColumn.getCurrentValue());
        if(range != null){
            if(range.getPath().equals(rightColumn.getName()))
                return range.getPredicate().execute(rightColumn.getCurrentValue());
            return range.getPredicate().execute(leftColumn.getCurrentValue());
        }
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

    public boolean insertRangeToJoinInfo(Range range){
        if(joinType.equals(JoinType.RIGHT) || joinType.equals(JoinType.LEFT)){
            if(leftColumn.getName().equals(range.getPath()) || rightColumn.getName().equals(range.getPath())){
                this.range = range;
                return true;
            }
        }
        return false;
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
