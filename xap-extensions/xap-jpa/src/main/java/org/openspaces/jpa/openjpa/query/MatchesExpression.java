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
 * Represents a LIKE expression in a JPQL query.
 *
 * @author Idan Moyal
 * @since 8.0.1
 */
public class MatchesExpression implements Expression, ExpressionNode {

    private static final long serialVersionUID = 1L;
    protected ExpressionNode _candidate;
    protected ExpressionNode _regularExpression;

    MatchesExpression(Value candidate, Value regularExpression) {
        this._candidate = (ExpressionNode) candidate;
        this._regularExpression = (ExpressionNode) regularExpression;
    }

    public void acceptVisit(ExpressionVisitor visitor) {
    }

    public void appendSql(StringBuilder sql) {
        _candidate.appendSql(sql);
        sql.append(" LIKE ");
        _regularExpression.appendSql(sql);
    }

    public NodeType getNodeType() {
        return NodeType.LIKE_EXPRESSION;
    }

}
