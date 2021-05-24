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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class RangeNode implements QueryOperationNode{
    private static final long serialVersionUID = 4429295310828611780L;

    private String fieldName;
    private Object value;
    private QueryOperator operator;
    private String functionName;

    public RangeNode() {}

    public RangeNode(String fieldName, Object value, QueryOperator operator, String functionName) {

        this.fieldName = fieldName;
        this.value = value;
        this.operator = operator;
        this.functionName = functionName;
    }

    @Override
    public List<QueryOperationNode> getChildren() {
        return Collections.emptyList();
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public void setOperator(QueryOperator operator) {
        this.operator = operator;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public String toString() {
        return operator.toString() +
               '(' + (functionName != null ? functionName : fieldName) +
               ", " + value +
               ')';
    }

    @Override
    public String getPrettifiedString() {
        String operatorString = ExplainPlanUtil.getQueryOperatorDescription( operator );

        return '(' + (functionName != null ? functionName : fieldName) + " " + operatorString +
                ExplainPlanUtil.getValueDesc( value ) +  ')';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(fieldName);
        out.writeObject(value);
        out.writeObject(operator);
        out.writeObject(functionName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.fieldName = (String) in.readObject();
        this.value = in.readObject();
        this.operator = (QueryOperator) in.readObject();
        this.functionName = (String) in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RangeNode rangeNode = (RangeNode) o;

        if (fieldName != null ? !fieldName.equals(rangeNode.fieldName) : rangeNode.fieldName != null)
            return false;
        if (value != null ? !value.equals(rangeNode.value) : rangeNode.value != null) return false;
        if (operator != rangeNode.operator) return false;
        return functionName != null ? functionName.equals(rangeNode.functionName) : rangeNode.functionName == null;

    }

    @Override
    public int hashCode() {
        int result = fieldName != null ? fieldName.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (operator != null ? operator.hashCode() : 0);
        result = 31 * result + (functionName != null ? functionName.hashCode() : 0);
        return result;
    }
}
