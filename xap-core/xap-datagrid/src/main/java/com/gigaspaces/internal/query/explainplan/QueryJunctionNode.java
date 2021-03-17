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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class QueryJunctionNode implements QueryOperationNode{

    private String name;
    private final List<QueryOperationNode> subTrees = new ArrayList<QueryOperationNode>();

    public QueryJunctionNode(){
    }

    public QueryJunctionNode(String name) {
        this.name = name;
    }

    public List<QueryOperationNode> getChildren() {
        return subTrees;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String getPrettifiedString() {
        StringBuilder result = new StringBuilder();
        final Iterator<QueryOperationNode> iterator = subTrees.iterator();
        while (iterator.hasNext()) {
            QueryOperationNode node = iterator.next();
            boolean surroundParentheses = isSurroundParentheses(node);
            appendNode(result, node, surroundParentheses);
            if (iterator.hasNext()) {
                result.append(" " + name + " ");
            }
        }

        return result.toString();
    }

    private boolean isSurroundParentheses(QueryOperationNode node) {
        if (node instanceof QueryJunctionNode) {
            QueryJunctionNode junctionNode = ((QueryJunctionNode) node);
            return junctionNode.getName().equals("AND") && isMoreThanOneRangeChildren(junctionNode);
        }
        return false;
    }

    private boolean isMoreThanOneRangeChildren(QueryJunctionNode junctionNode) {
        return junctionNode.subTrees.stream().filter(child -> !(child instanceof QueryJunctionNode)).count() > 1;
    }

    private void appendNode(StringBuilder result, QueryOperationNode node, boolean surroundParentheses) {
        if (surroundParentheses) {
            result.append("(");
        }
        result.append(node.getPrettifiedString());
        if (surroundParentheses) {
            result.append(")");
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(name);
        out.writeInt(subTrees.size());
        for (QueryOperationNode subTree : subTrees) {
            out.writeObject(subTree);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.name = (String) in.readObject();
        int size = in.readInt();
        for (int i=0; i < size; i++){
            subTrees.add((QueryOperationNode) in.readObject());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryJunctionNode that = (QueryJunctionNode) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return subTrees != null ? subTrees.equals(that.subTrees) : that.subTrees == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (subTrees != null ? subTrees.hashCode() : 0);
        return result;
    }
}
