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

import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Literal;
import org.apache.openjpa.kernel.exps.Path;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.ClassMetaData;

/**
 * Represents a literal value node in the transalated query expression tree.
 *
 * @author idan
 * @since 8.0
 */
public class LiteralValueNode implements Literal, ExpressionNode {
    //
    private static final long serialVersionUID = 1L;
    private int _ptype;
    private Object _value;

    public LiteralValueNode(Object value, int ptype) {
        _value = value;
        _ptype = ptype;
    }

    public void acceptVisit(ExpressionVisitor visitor) {
        visitor.enter(this);
        visitor.exit(this);
    }

    public String getAlias() {
        return null;
    }

    public ClassMetaData getMetaData() {
        return null;
    }

    public String getName() {
        return null;
    }

    public Path getPath() {
        return null;
    }

    public Value getSelectAs() {
        return null;
    }

    @SuppressWarnings("rawtypes")
    public Class getType() {
        return _value.getClass();
    }

    public boolean isAggregate() {
        return false;
    }

    public boolean isVariable() {
        return false;
    }

    public boolean isXPath() {
        return false;
    }

    public void setAlias(String alias) {
    }

    @SuppressWarnings("rawtypes")
    public void setImplicitType(Class type) {
    }

    public void setMetaData(ClassMetaData cm) {
    }

    public Object getValue(Object[] params) {
        return getValue();
    }

    public int getParseType() {
        return _ptype;
    }

    public Object getValue() {
        return _value;
    }

    public void setValue(Object value) {
        _value = value;
    }

    @Override
    public String toString() {
        if (_ptype == Literal.TYPE_STRING || _ptype == Literal.TYPE_SQ_STRING)
            return String.format("'%s'", _value.toString());
        return _value.toString();
    }

    public void appendSql(StringBuilder sql) {
        sql.append(toString());
    }

    public NodeType getNodeType() {
        return NodeType.LITERAL_VALUE;
    }

}
