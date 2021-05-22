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

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.jdbc.Stack;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.executor.EntriesCursor;
import com.j_spaces.jdbc.executor.IQueryExecutor;
import com.j_spaces.jdbc.executor.ScanCursor;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.QueryTableData;

import net.jini.core.transaction.Transaction;

import java.io.*;
import java.sql.SQLException;
import java.util.Collection;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * This is the main expression node. it holds to children, left and right, and has a few abstract
 * method that all subclasses must implement.
 *
 * @author Michael Mitrani, 2Train4
 */
public abstract class ExpNode
        implements Cloneable, SmartExternalizable {
    private static final long serialVersionUID = 1L;

    protected ExpNode leftChild, rightChild;

    protected QueryTemplatePacket template;

    /**
     * Empty constructor.
     */
    public ExpNode() {
        this(null, null);
    }

    /**
     * @param leftChild  The left child
     * @param rightChild The right child
     */
    public ExpNode(ExpNode leftChild, ExpNode rightChild) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }

    /**
     * Set the left child.
     */
    public void setLeftChild(ExpNode leftChild) {
        this.leftChild = leftChild;
    }

    /**
     * Set the right child.
     */
    public void setRightChild(ExpNode rightChild) {
        this.rightChild = rightChild;
    }

    /**
     * @return The left child
     */
    public ExpNode getLeftChild() {
        return leftChild;
    }

    /**
     * @return The right child
     */
    public ExpNode getRightChild() {
        return rightChild;
    }

    /**
     * Applies the consumer on each non-null child.
     */
    public void forEachChild(Consumer<ExpNode> consumer) {
        if (leftChild != null)
            consumer.accept(leftChild);
        if (rightChild != null)
            consumer.accept(rightChild);
    }

    /**
     * Applies the consumer on this node and its children recursively
     */
    public void traverse(Consumer<ExpNode> consumer) {
        Stack<ExpNode> stack = new Stack<>();
        stack.push(this);
        while (!stack.isEmpty()) {
            ExpNode curr = stack.pop();
            consumer.accept(curr);
            curr.forEachChild(stack::push);
        }
    }

    /**
     * Applies the consumer on this node and its children recursively in reverse (children before parent)
     */
    public void traverseReverse(Consumer<ExpNode> consumer) {
        for (ExpNode node : reverse()) {
            consumer.accept(node);
        }
    }

    /**
     * Generates a reverse tree/stack ensuring each node's children are popped before the node.
     */
    public Collection<ExpNode> reverse() {
        Stack<ExpNode> result = new Stack<>();
        traverse(result::push);
        return result;
    }

    /**
     * PreparedNodes will set their values through this methods. this one just calls the
     * prepareValues on the children
     */
    public void prepareValues(Object[] values) throws SQLException {
        if (leftChild != null)
            leftChild.prepareValues(values);
        if (rightChild != null)
            rightChild.prepareValues(values);
    }

    /**
     * PreparedNodes will set their values through this methods. this one just calls the
     * prepareValues on the children. This method will be used only for SQLTemplate.
     */
    public String prepareTemplateValues(TreeMap values, String colName)
            throws SQLException {
        if (this instanceof ColumnNode)
            colName = ((ColumnNode) this).getName();
        if (leftChild != null)
            colName = leftChild.prepareTemplateValues(values, colName);
        if (rightChild != null)
            colName = rightChild.prepareTemplateValues(values, colName);

        return colName;
    }


    /**
     * Operator nodes implement this method to return whether two objects satisfy the given
     * condition.
     *
     * @return true if the two given objects satisfy the condition
     */
    public abstract boolean isValidCompare(Object ob1, Object ob2);


    /**
     * is there somewhere in the tree a join condition.
     */
    public boolean isJoined() {
        return (getRightChild() instanceof ColumnNode) && (getLeftChild() instanceof ColumnNode);
    }


    /**
     * Prototype method - Create new instance of the class
     */
    public abstract ExpNode newInstance();

    /**
     * Override the clone method.
     */
    public Object clone() {
        ExpNode cloned = newInstance();

        if (this.leftChild != null)
            cloned.leftChild = (ExpNode) this.leftChild.clone();
        if (this.rightChild != null)
            cloned.rightChild = (ExpNode) this.rightChild.clone();

        if (template != null)
            cloned.setTemplate((QueryTemplatePacket) template.clone());
        return cloned;

    }

    /**
     * Accept the query builder Default implementation returns null - no QueryTemplatePacket is
     * defined
     */
    public abstract void accept(QueryTemplateBuilder builder) throws SQLException;

    /**
     * Accept the query executor
     */
    public void accept(IQueryExecutor executor, ISpaceProxy space, Transaction txn, int readModifier, int max) throws SQLException {
        executor.execute(this, space, txn, readModifier, max);
    }


    public QueryTemplatePacket getTemplate() {
        return template;
    }

    public void setTemplate(QueryTemplatePacket template) {
        this.template = template;
    }


    /**
     * @return op string
     */
    public String toString(String op) {
        StringBuilder b = new StringBuilder();

        b.append(leftChild != null ? leftChild.toString() : null);
        b.append(op);
        b.append(rightChild != null ? rightChild.toString() : null);

        return b.toString();

    }

    public EntriesCursor createIndex(QueryTableData queryTableData, IQueryResultSet<IEntryPacket> tableEntries) {
        return new ScanCursor(tableEntries);
    }

    /**
     * @return true if created join index
     */

    public boolean createJoinIndex(QueryTableData tableData) {
        QueryTableData leftTable = ((ColumnNode) getLeftChild()).getColumnData()
                .getColumnTableData();

        if (leftTable.equals(tableData)) {
            if (isJoined()) {
                tableData.join(this);
            } else {
                // create a filter condition
                if (getTemplate() != null
                        && tableData.getTableCondition() == null) {
                    tableData.setTableCondition(this);
                }
            }
        }
        return false;
    }

    /**
     * Gets whether this node has a child which is an inner query node
     */
    public boolean isInnerQuery() {
        return false;
    }

    /*
     * returns true if its a contains-item node 
     */
    public boolean isContainsItemNode() {
        return false;
    }

    /*
     * returns true if its a contains-items root node 
     */
    public boolean isContainsItemsRootNode() {
        return false;
    }

    /**
     * Validates inner query result size (this method is only called after executing the inner
     * query) Default validation only permits a single result from inner query.
     */
    public void validateInnerQueryResult() throws SQLException {
        InnerQueryNode innerQueryNode = (InnerQueryNode) rightChild;
        if (!innerQueryNode.isSingleResult() && !innerQueryNode.isEmptyResult()) {
            throw new SQLException("Inner query returned more than 1 rows/columns.");
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, rightChild);
        IOUtils.writeObject(out, leftChild);
        IOUtils.writeObject(out, template);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.rightChild = IOUtils.readObject(in);
        this.leftChild = IOUtils.readObject(in);
        this.template = IOUtils.readObject(in);
    }
}
