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
import org.apache.openjpa.kernel.exps.Parameter;
import org.apache.openjpa.kernel.exps.Path;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.ClassMetaData;

/**
 * Represents a parameter node in the translated query expression tree.
 *
 * @author idan
 * @since 8.0
 */
public class ParameterNode implements Parameter, ExpressionNode {
    //
    private static final long serialVersionUID = 1L;
    private int _index;
    private Class<?> _type;
    private ClassMetaData _classMetaData;

    @SuppressWarnings("rawtypes")
    public ParameterNode(Class type) {
        _type = type;
        _index = -1;
    }

    public void acceptVisit(ExpressionVisitor visitor) {
    }

    public String getAlias() {
        return null;
    }

    public ClassMetaData getMetaData() {
        return _classMetaData;
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
    }

    @SuppressWarnings("rawtypes")
    public void setImplicitType(Class class1) {
    }

    public void setMetaData(ClassMetaData cm) {
        _classMetaData = cm;
    }

    public Object getValue(Object[] aobj) {
        return null;
    }

    public Object getParameterKey() {
        return null;
    }

    public void setIndex(int index) {
        _index = index;
    }

    public int getIndex() {
        return _index;
    }

    public void appendSql(StringBuilder sql) {
        // We convert JPQL named parameters to "?" parameters which SQLQuery/JDBC supports.
        sql.append("?");
    }

    public NodeType getNodeType() {
        return NodeType.PARAMETER;
    }

}
