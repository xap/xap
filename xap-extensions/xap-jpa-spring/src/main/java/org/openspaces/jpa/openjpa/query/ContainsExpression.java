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
 * Represents a JPQL contains (MEMBER OF) expression.
 *
 * @author idan
 * @since 8.0
 */
public class ContainsExpression implements Expression, ExpressionNode {
    //
    private static final long serialVersionUID = 1L;
    private ExpressionNode _fieldPath = null;
    private ExpressionNode _value = null;

    public ContainsExpression(Value fieldPath, Value value) {
        this._fieldPath = (ExpressionNode) fieldPath;
        this._value = (ExpressionNode) value;
    }

    public void appendSql(StringBuilder sql) {
        _fieldPath.appendSql(sql);
        sql.append("[*] = ");
        _value.appendSql(sql);
    }

    public NodeType getNodeType() {
        return NodeType.CONTAINS_EXPRESSION;
    }

    public void acceptVisit(ExpressionVisitor visitor) {
    }

}
