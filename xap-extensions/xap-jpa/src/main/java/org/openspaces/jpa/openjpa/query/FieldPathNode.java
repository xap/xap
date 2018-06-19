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

import org.apache.openjpa.kernel.exps.Context;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Path;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.XMLMetaData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a field path in the expression tree. The {@link #get(FieldMetaData, boolean)} method
 * is called when passing through each ClassMetaData so this method saves the entire path to the
 * given column so it can be later converted to a string.
 *
 * @author idan
 * @since 8.0
 */
public class FieldPathNode implements Path, ExpressionNode {
    //
    private static final long serialVersionUID = 1L;
    private ClassMetaData _classMetaData;
    private FieldMetaData _fieldMetaData;
    private List<String> _path;
    private String _schemaAlias;
    private String _joinedFieldName = null;
    private boolean _collection = false;

    public FieldPathNode() {
        _path = new ArrayList<String>();
    }

    public void acceptVisit(ExpressionVisitor visitor) {
        visitor.enter(this);
        visitor.exit(this);
    }

    public String getAlias() {
        return null;
    }

    public ClassMetaData getMetaData() {
        return _classMetaData;
    }

    public String getName() {
        return toString();
    }

    public Path getPath() {
        return null;
    }

    public Value getSelectAs() {
        return null;
    }

    @SuppressWarnings("rawtypes")
    public Class getType() {
        return _fieldMetaData.getDeclaredType();
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

    public void setMetaData(ClassMetaData classMetaData) {
        _classMetaData = classMetaData;
    }

    public void get(FieldMetaData fmd, boolean nullTraversal) {
        _path.add(fmd.getName());
        _fieldMetaData = fmd;
    }

    public void get(FieldMetaData fieldMetaData, XMLMetaData xmlMetaData) {
    }

    public void get(XMLMetaData xmlMetaData, String name) {
    }

    public String getCorrelationVar() {
        return null;
    }

    public String getSchemaAlias() {
        return _schemaAlias;
    }

    public XMLMetaData getXmlMapping() {
        return null;
    }

    public FieldMetaData last() {
        return _fieldMetaData;
    }

    public void setSchemaAlias(String schemaAliasName) {
        _schemaAlias = schemaAliasName;
    }

    public void setSubqueryContext(Context arg0, String arg1) {
    }

    @Override
    public String toString() {
        if (_path.size() == 0)
            return "";
        StringBuilder path = new StringBuilder();
        Iterator<String> iterator = _path.iterator();
        path.append(iterator.next());
        while (iterator.hasNext()) {
            path.append(".");
            path.append(iterator.next());
        }
        return path.toString();
    }

    public void appendSql(StringBuilder sql) {
        if (_joinedFieldName == null) {
            sql.append(toString());
        } else {
            sql.append(_joinedFieldName);
            if (_collection)
                sql.append("[*]");
            sql.append(".");
            sql.append(toString());
        }
    }

    public NodeType getNodeType() {
        return NodeType.FIELD_PATH;
    }

    /**
     * In a JOIN statement - sets the JOINED property name.
     */
    public void setJoinedFieldName(String collectionName) {
        this._joinedFieldName = collectionName;
    }

    /**
     * Sets whether the JOINED property is a collection or not.
     */
    public void setCollection(boolean collection) {
        this._collection = collection;
    }

}
