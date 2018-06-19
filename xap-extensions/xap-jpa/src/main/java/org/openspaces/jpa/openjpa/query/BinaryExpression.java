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

package org.openspaces.jpa.openjpa.query;

import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Value;

/**
 * Represents a binary expression.
 *
 * @author idan
 * @since 8.0
 */
public class BinaryExpression implements Expression, ExpressionNode {
    //
    private static final long serialVersionUID = 1L;

    public enum ExpressionType {
        EQUAL, NOT_EQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL, AND, OR
    }

    private ExpressionNode _node1;
    private ExpressionNode _node2;
    private ExpressionType _expressionType;

    public BinaryExpression(Value value1, Value value2, ExpressionType expressionType) {
        _node1 = (ExpressionNode) value1;
        _node2 = (ExpressionNode) value2;
        _expressionType = expressionType;
    }

    public void appendSql(StringBuilder sql) {
        _node1.appendSql(sql);
        sql.append(this.toString());
        _node2.appendSql(sql);
    }

    public void acceptVisit(ExpressionVisitor visitor) {
    }

    @Override
    public String toString() {
        switch (_expressionType) {
            case EQUAL:
                return (_node2.getNodeType() == NodeType.NULL_VALUE) ? " IS " : " = ";
            case NOT_EQUAL:
                return (_node2.getNodeType() == NodeType.NULL_VALUE) ? " IS NOT " : " <> ";
            case GREATER_THAN:
                return " > ";
            case LESS_THAN:
                return " < ";
            case GREATER_THAN_OR_EQUAL:
                return " >= ";
            case LESS_THAN_OR_EQUAL:
                return " <= ";
        }
        return "";
    }

    public NodeType getNodeType() {
        return NodeType.BINARY_EXPRESSION;
    }

}
