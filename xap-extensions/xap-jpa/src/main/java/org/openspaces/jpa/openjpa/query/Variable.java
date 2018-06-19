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
import org.apache.openjpa.kernel.exps.Path;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.ClassMetaData;

/**
 * Represents a variable (Pojo) in the translated query expression tree. Initiated by the FROM
 * clause parser.
 *
 * @author idan
 * @since 8.0
 */
public class Variable implements Value, ExpressionNode {
    //
    private static final long serialVersionUID = 1L;
    private String _name;
    private String _alias;
    private Class<?> _type;
    private ClassMetaData _classMetaData;

    public Variable(String name, Class<?> type) {
        _name = name;
        _type = type;
        _classMetaData = null;
    }

    public void acceptVisit(ExpressionVisitor visitor) {
        visitor.enter(this);
        visitor.exit(this);
    }

    public String getAlias() {
        return _alias;
    }

    public ClassMetaData getMetaData() {
        return _classMetaData;
    }

    public String getName() {
        return _name;
    }

    public Path getPath() {
        return null;
    }

    public Value getSelectAs() {
        return null;
    }

    @SuppressWarnings("rawtypes")
    public Class getType() {
        return _type;
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
        _alias = alias;
    }

    @SuppressWarnings("rawtypes")
    public void setImplicitType(Class type) {
    }

    public void setMetaData(ClassMetaData classMetaData) {
        _classMetaData = classMetaData;
    }

    public void appendSql(StringBuilder sql) {
        sql.append(_classMetaData.getDescribedType().getName());

    }

    public NodeType getNodeType() {
        return NodeType.VARIABLE;
    }

}
