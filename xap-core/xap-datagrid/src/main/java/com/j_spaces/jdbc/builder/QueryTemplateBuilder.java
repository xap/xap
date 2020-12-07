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

/**
 *
 */
package com.j_spaces.jdbc.builder;

import com.gigaspaces.annotation.sql.SqlFunctionReturnType;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.CompoundContainsItemsCustomQuery;
import com.gigaspaces.internal.query.IContainsItemsCustomQuery;
import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.SQLFunctions;
import com.j_spaces.jdbc.SelectQuery;
import com.j_spaces.jdbc.builder.range.*;
import com.j_spaces.jdbc.parser.*;
import com.j_spaces.jdbc.query.QueryColumnData;
import com.j_spaces.jdbc.query.QueryTableData;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

/**
 * Builds an external entry from {@link ExpNode} The builder converts the expression tree to ranges.
 * The ranges are optimized , merged and attached to the expression node.
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class QueryTemplateBuilder implements Externalizable
{
    private static final long serialVersionUID = 1L;

    // The query object
    private AbstractDMLQuery query;

    // Required for Externalizable
    public QueryTemplateBuilder() {
    }

    /**
     *
     * @param query
     */
    public QueryTemplateBuilder(final AbstractDMLQuery query) {
        super();
        this.query = query;

    }

    /**
     * Build QueryTemplatePacket for contains. (syntax: "collection[*] = ?") Validate that the
     * column associated with the argument is not an embedded object's column.
     */
    public void buildContainsTemplate(ContainsNode containsNode) throws SQLException {

        if (containsNode.getRightChild() != null && containsNode.getRightChild().isContainsItemsRootNode()) {//contains item root. use its template set with the contains subtree
            ContainsItemsRootNode rn = (ContainsItemsRootNode) containsNode.getRightChild();
            if (rn.getTemplate() == null)
                throw new RuntimeException("invalid traverse tree state null template in containsItemsRoot");
            containsNode.setTemplate(rn.getTemplate());
            return;
        }

        QueryColumnData queryColumnData = ((ColumnNode) containsNode.getLeftChild()).getColumnData();
        FunctionCallDescription functionCallDescription = ((ColumnNode) containsNode.getLeftChild()).getFunctionCallDescription();

        // joined nodes are not built - executed on the fly
        if (containsNode.isJoined()) {
            if (query.isJoined())
                return;

            throwIllegalJoinExpressionException(containsNode);
        }


        // handle specific table name for column - used in join
        QueryTableData tableData = queryColumnData.getColumnTableData();
        ExpNode right = containsNode.getRightChild();
        ITypeDesc typeDesc = tableData.getTypeDesc();

        Object value = null;
        if(right instanceof PreparedNode) { //preparedNode is a literalNode so we have to check for relation here first, also it is NOT AbstractInNode...
            value = ((PreparedNode) right).getConvertedObject(typeDesc, containsNode.getPath());
        } else if (right instanceof LiteralNode) {
            value = ((LiteralNode) right).getValue();
        } else if (!(right instanceof IsNullNode)) {
            value = ((AbstractInNode) right).getConvertedValues(typeDesc, containsNode.getPath());
        }

        ContainsValueRange containsValueRange = containsNode.isRelationNode()
                        ? new ContainsValueRange(containsNode.getPath(), functionCallDescription, value, containsNode.getTemplateMatchCode(), containsNode.getRelation(), typeDesc.getTypeName())
                        : new ContainsValueRange(containsNode.getPath(), functionCallDescription, value, containsNode.getTemplateMatchCode());
        QueryTemplatePacket template = new QueryTemplatePacket(tableData, query.getQueryResultType(), queryColumnData.getColumnPath(), containsValueRange);

        containsNode.setTemplate(template);
    }

    /*
     * build query template packet for contains root node
     */
    public void buildContainsItemsRootTemplate(ContainsItemsRootNode rn)
            throws SQLException {
        QueryColumnData queryColumnData = rn.getRootColumnNode().getColumnData();

        // joined nodes are not built - executed on the fly
        if (rn.isJoined()) {
            if (query.isJoined())
                return;

            throwIllegalJoinExpressionException(rn);
        }


        // handle specific table name for column - used in join
        QueryTableData tableData = queryColumnData.getColumnTableData();

        QueryTemplatePacket template = new QueryTemplatePacket(tableData, query.getQueryResultType(),
                buildContainsItemsRootQuery(rn, null));

        rn.setTemplate(template);


    }

    private IContainsItemsCustomQuery buildContainsItemsRootQuery(ContainsItemsRootNode rn, String path) throws SQLException {//build the contains items logic from the contains tree pointed by ContainsItemsRootNode
        List<IContainsItemsCustomQuery> subs = new LinkedList<IContainsItemsCustomQuery>();

        String fullPath = rn.getRoot();
        if (path != null) {
            StringBuilder sb = new StringBuilder(path);
            sb.append(".");
            sb.append(rn.getRoot());
            fullPath = sb.toString();
        }
        ExpNode exp = rn.getContainsSubtree();
        if (exp.isContainsItemNode() || exp.isContainsItemsRootNode()) {
            buildContainsItemsBasicQuery(exp, fullPath, subs);
        } else {
            if (exp.getLeftChild() != null) {
                buildContainsItemsBasicQuery(exp.getLeftChild(), fullPath, subs);
            }
            if (exp.getRightChild() != null) {
                buildContainsItemsBasicQuery(exp.getRightChild(), fullPath, subs);
            }
        }
        return new CompoundContainsItemsCustomQuery(rn.getRoot(), fullPath, subs);
    }

    private void buildContainsItemsBasicQuery(ExpNode node, String fullPath, List<IContainsItemsCustomQuery> subs) throws SQLException {
        if (node.isContainsItemsRootNode()) {
            subs.add(buildContainsItemsRootQuery((ContainsItemsRootNode) node, fullPath));
            return;
        }
        if (node.isContainsItemNode() && !node.getRightChild().isContainsItemsRootNode()) {
            ContainsItemNode cn = (ContainsItemNode) node;
            StringBuilder sb = new StringBuilder(fullPath);
            sb.append(".");
            sb.append(cn.getPath());
            fullPath = sb.toString();

            ExpNode right = node.getRightChild();


            Object value = null;
            if(right instanceof PreparedNode && cn.isRelationNode()) { //preparedNode is a literalNode so we have to check for relation here first, also it is NOT AbstractInNode...
                value = ((PreparedNode) right).getConvertedObject(cn.getTypeDesc(), fullPath);
            } else
            if (right instanceof LiteralNode) {
                value = ((LiteralNode) right).getValue();
            } else {
                value = ((AbstractInNode) right).getConvertedValues(query.getTypeInfo(), fullPath);
            }

            ContainsItemValueRange containsItemValueRange = cn.isRelationNode()
                    ? new ContainsItemValueRange(cn.getPath(), fullPath, null, value, cn.getTemplateMatchCode(), cn.getRelation(),cn.getTypeDesc().getTypeName())
                    : new ContainsItemValueRange(cn.getPath(), fullPath, null, value, cn.getTemplateMatchCode());

            subs.add(containsItemValueRange);
            return;
        }

        if (node.getLeftChild() != null) {
            buildContainsItemsBasicQuery(node.getLeftChild(), fullPath, subs);
        }
        if (node.getRightChild() != null) {
            buildContainsItemsBasicQuery(node.getRightChild(), fullPath, subs);
        }

    }


    /**
     * Build QueryTemplatePacket from {@link ExpNode}
     */
    public void build(ExpNode node, short op, short nullOp) throws SQLException {

        // joined nodes are not built - executed on the fly
        if (node.isJoined()) {
            if (query.isJoined())
                return;

            throwIllegalJoinExpressionException(node);
        }
        ColumnNode col = ((ColumnNode) node.getLeftChild());
        //todo barak add functionName to all other buildTemplate
        QueryTemplatePacket template = buildTemplate((LiteralNode) node.getRightChild(), col.getColumnData(), op, nullOp, col.getFunctionCallDescription(),node);
        node.setTemplate(template);
    }

    public void build(RelationNode node) throws SQLException {
//		QueryTemplatePacket template = buildTemplate((LiteralNode)node.getRightChild(), col.getColumnData(), op, nullOp);
        String[] relations = node.getRelation().split(":");
        String namespace = relations[0];
        String op = relations[1];
        QueryTemplatePacket template = buildTemplate((ColumnNode) node.getLeftChild(), namespace, op, (LiteralNode) node.getRightChild());
        node.setTemplate(template);

    }

    /**
     * Build QueryTemplatePacket from {@link ExpNode}
     */
    public void build(ExpNode node, short op)
            throws SQLException {
        build(node, op, op);
    }

    /**
     * Build QueryTemplatePacket from {@link LikeNode} (special conversion)
     */
    public void build(LikeNode node, short op)
            throws SQLException {

        if (node.isJoined()) {
            throwIllegalJoinExpressionException(node);
        }

        ColumnNode col = ((ColumnNode) node.getLeftChild());
        QueryTableData tableData = col.getColumnData().getColumnTableData();

        Object obj = ((LiteralNode) node.getRightChild()).getConvertedObject(tableData.getTypeDesc(), col.getColumnPath());

        String regex = ((String) obj).replaceAll("%", ".*").replaceAll("_",
                ".");

        Range range = toRange(col.getColumnPath(), col.getFunctionCallDescription(), regex, TemplateMatchCodes.REGEX);
        node.setTemplate(new QueryTemplatePacket(tableData, query.getQueryResultType(), col.getColumnPath(), range));
    }

    /**
     * Build QueryTemplatePacket from {@link InNode} (special conversion)
     */
    public void build(InNode node)
            throws SQLException {

        QueryTemplatePacket template = buildTemplate(node, query);

        node.setTemplate(template);

    }

    /**
     * Build QueryTemplatePacket from {@link NotInNode} (special conversion)
     */
    public void build(NotInNode node)
            throws SQLException {

        QueryTemplatePacket template = buildTemplate(node, query);

        node.setTemplate(template);

    }


    /**
     * @return QueryTemplatePacket
     */
    public static QueryTemplatePacket buildTemplate(NotInNode node, AbstractDMLQuery query)
            throws SQLException {
        ColumnNode column = (ColumnNode) node.getLeftChild();

        QueryTableData tableData = column.getColumnData().getColumnTableData();

        // Build a template with composite ranges
        // x not in (a,b,c,null) ---> (x <> a) && (x <> b)&& (x <>t c)&& (x is not null)
        // the first query is executed, the others validate the result
        String colName = column.getColumnPath();

        CompositeRange range = new CompositeRange(colName);
        Set<Object> notInValues = node.getConvertedValues(tableData.getTypeDesc(), colName);

        for (Object notInValue : notInValues) {
            if (notInValue == null)
                range.add(new NotNullRange(colName, column.getFunctionCallDescription()));
            else {
                range.add(new NotEqualValueRange(colName, column.getFunctionCallDescription(), notInValue));
            }
        }

        QueryTemplatePacket template = new QueryTemplatePacket(tableData, query.getQueryResultType(), colName, range);
        return template;
    }

    /**
     * @return QueryTemplatePacket
     */
    public static QueryTemplatePacket buildTemplate(InNode node, AbstractDMLQuery query) throws SQLException {
        // check if the field type is a uid,
        // if so - use the multiple uids feature
        ColumnNode column = (ColumnNode) node.getLeftChild();

        QueryTableData tableData = column.getColumnData().getColumnTableData();

        ITypeDesc info = tableData.getTypeDesc();
        String idPropertyName = info.getIdPropertyName();
        if (idPropertyName != null && info.isAutoGenerateId() && column.getColumnPath().equals(idPropertyName)) {
            String[] colNames = new String[]{column.getColumnPath()};

            // Go over the IN values and create a uid array
            Set<String> uids = node.getConvertedValues(info, colNames[0]);

            Range range = new UidsRange(column.getColumnPath(), column.getFunctionCallDescription(), uids);
            return new QueryTemplatePacket(tableData, query.getQueryResultType(), column.getColumnPath(), range);
        }

        // Go over the IN values and and create a packet that will be executed later
        ITypeDesc typeDesc = tableData.getTypeDesc();
        Set<Object> inValues = node.getConvertedValues(typeDesc, column.getColumnData().getColumnPath());

        Range range = new InRange(column.getColumnPath(), column.getFunctionCallDescription(), inValues);
        return new QueryTemplatePacket(tableData, query.getQueryResultType(), column.getColumnPath(), range);

    }


    /**
     * Build space template
     */
    public QueryTemplatePacket buildTemplate(LiteralNode node, QueryColumnData queryColumnData,
                                             short op, short nullOp, FunctionCallDescription functionCallDescription,ExpNode father) throws SQLException {
        QueryTableData tableData = queryColumnData.getColumnTableData();

        ITypeDesc typeDesc = tableData.getTypeDesc();
        Object value;
        if (functionCallDescription != null && SQLFunctions.isBuiltIn(functionCallDescription.getName()) &&
                SQLFunctions.getBuiltInFunction(functionCallDescription.getName()).getClass().isAnnotationPresent(SqlFunctionReturnType.class)) {
            value = ObjectConverter.convert(node.getValue(), SQLFunctions.getBuiltInFunction(functionCallDescription.getName()).getClass().getAnnotation(SqlFunctionReturnType.class).type());
        } else {
            value = node.getConvertedObject(typeDesc, queryColumnData.getColumnPath());
        }

        return new QueryTemplatePacket(tableData, query.getQueryResultType(), queryColumnData.getColumnPath(), toRange(queryColumnData.getColumnPath(), functionCallDescription, value, value == null ? nullOp : op,father,query));
    }

    private QueryTemplatePacket buildTemplate(ColumnNode columnNode, String namespace, String op, LiteralNode valueNode) throws SQLException {
        QueryColumnData columnData = columnNode.getColumnData();
        QueryTableData tableData = columnData.getColumnTableData();
        ITypeDesc typeDesc = tableData.getTypeDesc();
        Object value = valueNode.getConvertedObject(typeDesc, columnData.getColumnPath());
        return new QueryTemplatePacket(tableData, query.getQueryResultType(), columnData.getColumnPath(), new RelationRange(typeDesc.getTypeName(), columnData.getColumnPath(), value, namespace, op));
    }

    /**
     * Converts single external entry expression to a range
     *
     * @return Range
     */
    @SuppressWarnings("deprecation")
    public static Range toRange(String colName, FunctionCallDescription functionCallDescription, Object value, short matchCode) {
        return toRange( colName, functionCallDescription, value,  matchCode,null,null);
    }

    @SuppressWarnings("deprecation")
    public static Range toRange(String colName, FunctionCallDescription functionCallDescription, Object value, short matchCode,ExpNode node,AbstractDMLQuery query) {
        switch (matchCode) {
            case TemplateMatchCodes.IS_NULL:
                return new IsNullRange(colName, functionCallDescription);
            case TemplateMatchCodes.NOT_NULL:
                return new NotNullRange(colName, functionCallDescription);
            case TemplateMatchCodes.EQ: {
                if (node != null) {
                    ColumnNode column = (ColumnNode) node.getLeftChild();
                    QueryTableData tableData = column.getColumnData().getColumnTableData();
                    ITypeDesc info = tableData.getTypeDesc();
                    String idPropertyName = info.getIdPropertyName();
                    if (idPropertyName != null && info.isAutoGenerateId() && column.getColumnPath().equals(idPropertyName)) {
                        String uid = (String)((LiteralNode) node.getRightChild()).getValue();
                        Set<String> uids = new HashSet<String>();
                        uids.add(uid);
                        return  new UidsRange(column.getColumnPath(), column.getFunctionCallDescription(), uids);
                    }
                }
                return new EqualValueRange(colName, functionCallDescription, value);
            }
            case TemplateMatchCodes.NE:
                return new NotEqualValueRange(colName, functionCallDescription, value);

            case TemplateMatchCodes.GT:
                return new SegmentRange(colName, functionCallDescription, castToComparable(value), false, null, false);
            case TemplateMatchCodes.GE:
                return new SegmentRange(colName, functionCallDescription, castToComparable(value), true, null, false);
            case TemplateMatchCodes.LE:
                return new SegmentRange(colName, functionCallDescription, null, false, castToComparable(value), true);
            case TemplateMatchCodes.LT:
                return new SegmentRange(colName, functionCallDescription, null, false, castToComparable(value), false);

            case TemplateMatchCodes.REGEX:
                return new RegexRange(colName, functionCallDescription, (String) value);
            case TemplateMatchCodes.NOT_REGEX:
                return new NotRegexRange(colName, functionCallDescription, (String) value);
            case TemplateMatchCodes.IN:
                return new InRange(colName, functionCallDescription, (Set) value);
            case TemplateMatchCodes.RELATION:
                throw new UnsupportedOperationException("Relation is not supported with index");
        }

        return Range.EMPTY_RANGE;
    }
    /**
     * Cast the object to Comparable otherwise throws an IllegalArgumentException exception
     */
    private static Comparable castToComparable(Object obj) {
        try {
            //NOTE- a check for Comparable interface implementation is be done in the proxy
            return (Comparable) obj;
        } catch (ClassCastException cce) {
            throw new IllegalArgumentException("Type " + obj.getClass() +
                    " doesn't implement Comparable, Serialization mode might be different than " + StorageType.OBJECT + ".", cce);
        }
    }

    /**
     * Build QueryTemplatePacket from {@link AndNode}
     */
    public void build(AndNode node)
            throws SQLException {

        // for now joined queries are handled differently - only leaves are built - to keep the tree structure
//        if (query.isJoined()) {
//            return;
//        }
        // Handle the case of one child - just delegate
        ExpNode left = node.getLeftChild();
        ExpNode right = node.getRightChild();


        // handle single child case
        if (left == null) {
            node.setTemplate(right.getTemplate());
            return;
        }

        if (right == null) {
            node.setTemplate(left.getTemplate());
            return;
        }

        // Handle the case of two children
        QueryTemplatePacket leftTemplate = left.getTemplate();
        QueryTemplatePacket rightTemplate = right.getTemplate();

        // if one of the children can't be translated to a space template
        // attach the templates to the children
        if (leftTemplate == null && rightTemplate == null) {
            return;
        }

        // check for intersection with an empty template
        if (leftTemplate != null && leftTemplate.isAlwaysEmpty()) {
            node.setTemplate(leftTemplate);
            return;
        }

        if (rightTemplate != null && rightTemplate.isAlwaysEmpty()) {
            node.setTemplate(rightTemplate);
            return;
        }

        // handle the case when one of the children can't be translated to a space template
        if (leftTemplate == null || rightTemplate == null) {
            return;
        }

        // Now try to convert two templates to one
        node.setTemplate(leftTemplate.buildAndPacket(rightTemplate));

    }


    /**
     * Build QueryTemplatePacket from {@link OrNode}
     */
    public void build(OrNode node)
            throws SQLException {
        // Handle the case of one child - just delegate
        ExpNode left = node.getLeftChild();
        ExpNode right = node.getRightChild();


        // for now joined queries are handled differently - only leaves are built - to keep the tree structure
//        if (query.isJoined()) {
//            return;
//        }


        if (left == null) {
            node.setTemplate(right.getTemplate());
            return;
        }

        if (right == null) {
            node.setTemplate(left.getTemplate());
            return;
        }

        // Handle the case of two children
        QueryTemplatePacket leftTemplate = left.getTemplate();
        QueryTemplatePacket rightTemplate = right.getTemplate();


        // if one of the children can't be translated to a space template
        // attach the templates to the children
        if (leftTemplate == null && rightTemplate == null) {

            return;
        }

        // check for union with an empty template - return the other template
        if (leftTemplate != null && leftTemplate.isAlwaysEmpty()) {
            node.setTemplate(rightTemplate);
            return;
        }

        if (rightTemplate != null && rightTemplate.isAlwaysEmpty()) {
            node.setTemplate(leftTemplate);
            return;
        }

        // handle the case where both sides of OR can be executed as a single query to space
        if (leftTemplate != null && rightTemplate != null) {
            node.setTemplate(leftTemplate.buildOrPacket(rightTemplate));
        }
    }

    /**
     * Traverse the binary expression tree non-recursively using a custom stack The tree has to be
     * traversed in postorder - the parent is traversed after its children.
     */
    public void traverseExpressionTree(ExpNode root, boolean buildJoinInfo) throws SQLException {
        if (root != null) {
            for (ExpNode node : root.reverse()) {
                if (node.isContainsItemsRootNode()) {
                    ContainsItemsRootNode containsItemsRootNode = (ContainsItemsRootNode) node;
                    traverseSubItemNodes(containsItemsRootNode.getContainsSubtree(),containsItemsRootNode.createTypeDesc());
                }

                node.accept(this);

                // check if the template should be converted to space format
                if (node.getTemplate() == null) {
                    // convert the children of complex nodes(join for example)
                    node.forEachChild(c -> {
                        if (c.getTemplate() != null)
                            c.getTemplate().prepareForSpace(query.getTypeInfo());
                    });
                } else {
                    // if is root and has a template - convert it
                    if (node == root) {
                        node.getTemplate().prepareForSpace(query.getTypeInfo());
                    }
                }
            }
        }

        if (buildJoinInfo && query.isJoined()) {
            buildJoinInfo();
        }
    }

    private void traverseSubItemNodes(ExpNode node, ITypeDesc typeDesc) throws SQLException {
        if(node.isContainsItemNode()){
            if(((ContainsItemNode)node).isRelationNode())
                ((ContainsItemNode)node).setTypeDesc(typeDesc);
            return;
        }
        if(node.getLeftChild() != null){
            traverseSubItemNodes(node.getLeftChild(),typeDesc);
        }
        if(node.getRightChild() != null){
            traverseSubItemNodes(node.getRightChild(),typeDesc);
        }
    }

    /**
     * Create join related data structures and relations between tables
     */
    private void buildJoinInfo() {
        // for each table create a join condition
        for (QueryTableData tableData : query.getTablesData()) {
            // first all entries for each table in the query
            if (query instanceof SelectQuery && SelectQuery.pushDownPredicatesToSpace) {
                if (tableData.getExpTree() != null)
                    tableData.createJoinIndex(tableData.getExpTree());
            } else {
                tableData.createJoinIndex(query.getExpTree());
            }
        }


        List<LinkedList<QueryTableData>> joinedSequences = new LinkedList<LinkedList<QueryTableData>>();

        for (int i = 0; i < query.getTablesData().size(); i++) {
            QueryTableData tableData = query.getTablesData().get(i);


            //check for sequence beginning
            if (!tableData.isJoined()) {
                LinkedList<QueryTableData> seq = new LinkedList<QueryTableData>();
                seq.add(tableData);

                while (tableData.getJoinTable() != null) {
                    tableData = tableData.getJoinTable();
                    seq.add(tableData);
                }
                joinedSequences.add(seq);
            }

        }


        QueryTableData lastJoined = null;
        // link the sequences to a single instance
        for (LinkedList<QueryTableData> seq : joinedSequences) {
            if (lastJoined != null) {
                // link
                lastJoined.setJoinTable(seq.getFirst());
                seq.getFirst().setJoined(true);
            }

            lastJoined = seq.getLast();
        }

    }


    /**
     * Special build for not like - since it can't be translated to a space template. The query
     */
    public void build(NotLikeNode node) throws SQLException {
        if (node.isJoined())
            throwIllegalJoinExpressionException(node);

        ColumnNode col = (ColumnNode) node.getLeftChild();
        QueryTableData tableData = col.getColumnData().getColumnTableData();

        Object value = ((LiteralNode) node.getRightChild()).getConvertedObject(tableData.getTypeDesc(), col.getColumnPath());

        String regex = ((String) value).replaceAll("%", ".*").replaceAll("_", ".");

        NotRegexRange range = new NotRegexRange(col.getColumnPath(), col.getFunctionCallDescription(), regex);
        node.setTemplate(new QueryTemplatePacket(tableData, query.getQueryResultType(), col.getColumnPath(), range));
    }


    /**
     * @param node
     * @throws SQLException
     */
    private void throwIllegalJoinExpressionException(ExpNode node)
            throws SQLException {
        throw new SQLException("Right-hand expression of=[" + node + "] must be a constant value - '" + node.getRightChild() + "'.");
    }


    /**
     * Build QueryTemplatePacket from {@link AndNode}
     */
    public void build(ColumnNode node)
            throws SQLException {
        //setTableData(node);
        node.createColumnData(query);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, query);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        query = IOUtils.readObject(in);
    }
}
