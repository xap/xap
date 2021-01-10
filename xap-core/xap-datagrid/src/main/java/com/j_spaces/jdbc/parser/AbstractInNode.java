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

package com.j_spaces.jdbc.parser;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.jdbc.ResultEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractInNode extends ExpNode {
    private static final long serialVersionUID = 1L;

    protected HashSet<LiteralNode> valuesList;

    public AbstractInNode() {
        super();
    }

    public void prepareValues(Object[] values) throws SQLException {
        if (valuesList != null && values != null) {
            // Set values for prepared nodes
            for (LiteralNode valueNode : valuesList) {
                if (valueNode.isPreparedValue())
                    valueNode.setValue(values[((PreparedNode) valueNode).getIndex() - 1]);
            }
        }
    }

    public AbstractInNode(ExpNode leftChild, ExpNode rightChild) {
        super(leftChild, rightChild);
    }

    public void setValuesList(HashSet<LiteralNode> valuesList) {
        this.valuesList = valuesList;
    }

    public Set<LiteralNode> getValuesList() {
        return valuesList;
    }

    public <T> Set<T> getConvertedValues(ITypeDesc typeDesc, String propertyName)
            throws SQLException {
        HashSet<T> convertedValues = new HashSet<T>();
        for (LiteralNode value : valuesList) {
            Object convertedObject = value.getConvertedObject(typeDesc,
                    propertyName);

            if (convertedObject == null)
                convertedValues.add(null);
            else if (convertedObject instanceof Collection)
                convertedValues.addAll((Collection) convertedObject);
            else if (convertedObject.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(convertedObject); i++) {
                    convertedValues.add((T) Array.get(convertedObject, i));
                }

            } else
                convertedValues.add((T) convertedObject);
        }
        return convertedValues;
    }

    /**
     * Override the clone method.
     */
    public Object clone() {
        AbstractInNode cloned = (AbstractInNode) super.clone();

        if (getRightChild() != null && getRightChild().isInnerQuery())
            cloned.setRightChild((InnerQueryNode) getRightChild().clone());
        else {
            // perform a deep clone
            // so the preapred nodes are cloned as well
            HashSet<LiteralNode> clonedSet = new HashSet<LiteralNode>();

            for (LiteralNode value : valuesList) {
                clonedSet.add((LiteralNode) value.clone());
            }
            cloned.setValuesList(clonedSet);
        }
        return cloned;
    }

    @Override
    public void validateInnerQueryResult() throws SQLException {
        // IN/NOT IN clause only accepts single column result from inner query
        InnerQueryNode innerQueryNode = (InnerQueryNode) rightChild;
        if (!innerQueryNode.isSingleColumnResult()) {
            throw new SQLException("Inner query returned more than 1 row.");
        }
        // Convert inner query results to literal nodes
        valuesList = new HashSet<LiteralNode>();
        ResultEntry innerResult = innerQueryNode.getResult();
        for (int i = 0; i < innerResult.getFieldValues().length; i++) {
            valuesList.add(new LiteralNode(innerResult.getFieldValues()[i][0]));
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, valuesList);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.valuesList = IOUtils.readObject(in);
    }
}