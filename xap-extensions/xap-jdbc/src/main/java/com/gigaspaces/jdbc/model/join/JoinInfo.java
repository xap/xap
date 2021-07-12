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
package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.statement.select.Join;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.Objects;

public class JoinInfo {

    private final IQueryColumn leftColumn;
    private final IQueryColumn rightColumn;
    private final JoinType joinType;
    private Range range;
    private boolean hasMatch;

    public JoinInfo(IQueryColumn leftColumn, IQueryColumn rightColumn, JoinType joinType) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.joinType = joinType;
    }

    public boolean checkJoinCondition(){
        Object rightValue = rightColumn.getCurrentValue();
        Object leftValue = leftColumn.getCurrentValue();
        if(joinType.equals(JoinType.INNER) || joinType.equals(JoinType.SEMI)) {
            hasMatch = rightValue != null && leftValue != null && Objects.equals(leftValue, rightValue);
        } else if(range != null){
            if(range.getPath().equals(rightColumn.getName())) {
                hasMatch = range.getPredicate().execute(rightValue);
            }
            else {
                hasMatch = range.getPredicate().execute(leftValue);
            }
        } else {
            hasMatch = true;
        }
        return hasMatch;
    }

    public boolean isHasMatch() {
        return hasMatch;
    }

    public IQueryColumn getLeftColumn() {
        return leftColumn;
    }

    public IQueryColumn getRightColumn() {
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

    public void resetHasMatch() {
        hasMatch = false;
    }

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL, SEMI;

        public static JoinType getType(Join join){
            if(join.isLeft())
                return LEFT;
            if(join.isRight())
                return RIGHT;
            if (join.isOuter() || join.isFull())
                return FULL;
            if(join.isSemi()) {
                return SEMI;
            }
            return INNER;
        }

        public static JoinType getType(JoinRelType joinRelType){
            switch (joinRelType) {
                case INNER:
                    return INNER;
                case LEFT:
                    return LEFT;
                case RIGHT:
                    return RIGHT;
                case FULL:
                    return FULL;
                case SEMI:
                    return SEMI;
                default:
                    throw new UnsupportedOperationException("Join of type " + joinRelType + " is not supported");
            }
        }

        public static byte toCode(JoinType joinType) {
            if (joinType == null)
                return 0;
            switch (joinType) {
                case INNER: return 1;
                case LEFT: return 2;
                case RIGHT: return 3;
                case FULL: return 4;
                case SEMI: return 5;
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
                case 5: return SEMI;
                default: throw new IllegalArgumentException("Unsupported join code: " + code);
            }
        }
    }

    public enum JoinAlgorithm {
        Nested, Hash, SortMerge
    }
}
