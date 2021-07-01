package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.UnionTemplatePacket;
import com.j_spaces.jdbc.builder.range.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WhereHandler extends RexShuttle {
    private final RexProgram program;
    private final QueryTemplatePacketsHolder queryTemplatePacketsHolder;
    private final List<RelDataTypeField> inputFields;
    private final Map<String, QueryTemplatePacket> queryTemplatePackets = new LinkedHashMap<>();

    public WhereHandler(RexProgram program, QueryTemplatePacketsHolder queryTemplatePacketsHolder,
                        List<RelDataTypeField> inputFields) {
        this.program = program;
        this.queryTemplatePacketsHolder = queryTemplatePacketsHolder;
        this.inputFields = inputFields;
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

    public Map<String, QueryTemplatePacket> getQTPMap() {
        return queryTemplatePackets;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        handleRexCall(call);
        return call;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
        final RexNode node = getNode(localRef);
        if (!(node instanceof RexLocalRef)) {
            node.accept(this);
        }
        return localRef;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        if (inputRef.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
            RelDataTypeField field = inputFields.get(inputRef.getIndex());
            QueryTemplatePacket qtp = getQueryTemplatePacketForField(field);
            Range range = new EqualValueRange(field.getName(), true); // always equality to true, otherwise the path goes  through the handleRexCall method.
            QueryTemplatePacket newQtp = new QueryTemplatePacket(qtp);
            newQtp.getRanges().put(range.getPath(), range);
            getQTPMap().put(newQtp.getTypeName(), newQtp);
        }
        return inputRef;
    }

    private void handleRexCall(RexCall call) {

        switch (call.getKind()) {
            case AND: {
                WhereHandler leftHandler = new WhereHandler(program, queryTemplatePacketsHolder, inputFields);
                RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
                leftOp.accept(leftHandler);
                for (int i = 1; i < call.getOperands().size(); i++) {
                    RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(i));
                    WhereHandler rightHandler = new WhereHandler(program, queryTemplatePacketsHolder, inputFields);
                    rightOp.accept(rightHandler);
                    and(leftHandler, rightHandler);
                }
                break;
            }
            case OR: {
                WhereHandler leftHandler = new WhereHandler(program, queryTemplatePacketsHolder, inputFields);
                RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
                leftOp.accept(leftHandler);
                for (int i = 1; i < call.getOperands().size(); i++) {
                    RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(i));
                    WhereHandler rightHandler = new WhereHandler(program, queryTemplatePacketsHolder, inputFields);
                    rightOp.accept(rightHandler);
                    or(leftHandler, rightHandler);
                }
                break;
            }
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
                RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(1));
                handleTwoOperandsCall(leftOp, rightOp, call.getKind());
                break;
            case IS_NULL:
            case IS_NOT_NULL:
            case NOT:
                handleSingleOperandsCall(getNode((RexLocalRef) call.getOperands().get(0)), call.getKind());
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported", call.getKind()));
        }
    }

    private void handleSingleOperandsCall(RexNode operand, SqlKind sqlKind) {
        RelDataTypeField field = null;
        Range range = null;
        switch (operand.getKind()) {
            case INPUT_REF:
                field = inputFields.get(((RexInputRef) operand).getIndex());
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported", operand.getKind()));
        }
        QueryTemplatePacket qtp = getQueryTemplatePacketForField(field);
        switch (sqlKind) {
            case IS_NULL:
                range = new IsNullRange(field.getName());
                if (queryTemplatePacketsHolder.addRangeToJoinInfo(qtp.getTypeName(), range)) {
                    return;
                }
                break;
            case IS_NOT_NULL:
                range = new NotNullRange(field.getName());
                if (queryTemplatePacketsHolder.addRangeToJoinInfo(qtp.getTypeName(), range)) {
                    return;
                }
                break;
            case NOT:
                if (!operand.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
                    throw new UnsupportedOperationException("Queries with NOT on non-boolean column are not supported yet");
                }
                range = new EqualValueRange(field.getName(), false);
                if (queryTemplatePacketsHolder.addRangeToJoinInfo(qtp.getTypeName(), range)) {
                    return;
                }
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported", sqlKind));
        }
        QueryTemplatePacket newQtp = new QueryTemplatePacket(qtp);
        newQtp.getRanges().put(range.getPath(), range);
        getQTPMap().put(newQtp.getTypeName(), newQtp);
    }

    private void handleTwoOperandsCall(RexNode leftOp, RexNode rightOp, SqlKind sqlKind) {
        Object value = null;
        RelDataTypeField field = null;
        boolean isRowNum = false;
        Range range = null;
        switch (leftOp.getKind()) {
            case LITERAL:
                value = getValue((RexLiteral) leftOp);
            case INPUT_REF:
                field = inputFields.get(((RexInputRef) leftOp).getIndex());
                break;
            case CAST:
                handleTwoOperandsCall(getNode((RexLocalRef) ((RexCall) leftOp).getOperands().get(0)), rightOp, sqlKind);
                return; //return from recursion
            case DYNAMIC_PARAM:
                value = queryTemplatePacketsHolder.getPreparedValues()[((RexDynamicParam) leftOp).getIndex()];
                break;
            case ROW_NUMBER:
                isRowNum = true;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported", sqlKind));
        }
        switch (rightOp.getKind()) {
            case LITERAL:
                value = getValue((RexLiteral) rightOp);
                break;
            case INPUT_REF:
                field = inputFields.get(((RexInputRef) rightOp).getIndex());
                break;
            case CAST:
                handleTwoOperandsCall(leftOp, getNode((RexLocalRef) ((RexCall) rightOp).getOperands().get(0)), sqlKind);
                return; //return from recursion
            case DYNAMIC_PARAM:
                value = queryTemplatePacketsHolder.getPreparedValues()[((RexDynamicParam) rightOp).getIndex()];
                break;
            case ROW_NUMBER:
                isRowNum = true;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported", sqlKind));
        }
        if (isRowNum) {
            handleRowNumber(sqlKind, value);
            return; //return and don't continue.
        }

        if (field == null) {
            throw new IllegalStateException("Field is null");
        }
        QueryTemplatePacket qtp = getQueryTemplatePacketForField(field);
        try {
            value = SQLUtil.cast(qtp.getTypeDescriptor(), field.getName(), value, false);
        } catch (SQLException e) {
            throw new SQLExceptionWrapper(e);//throw as runtime.
        }
        switch (sqlKind) {
            case EQUALS:
                range = new EqualValueRange(field.getName(), value);
                break;
            case NOT_EQUALS:
                range = new NotEqualValueRange(field.getName(), value);
                break;
            case LESS_THAN:
                range = new SegmentRange(field.getName(), null, false, (Comparable) value, false);
                break;
            case LESS_THAN_OR_EQUAL:
                range = new SegmentRange(field.getName(), null, false, (Comparable) value, true);
                break;
            case GREATER_THAN:
                range = new SegmentRange(field.getName(), (Comparable) value, false, null, false);
                break;
            case GREATER_THAN_OR_EQUAL:
                range = new SegmentRange(field.getName(), (Comparable) value, true, null, false);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported", sqlKind));
        }
        QueryTemplatePacket newQtp = new QueryTemplatePacket(qtp);
        newQtp.getRanges().put(range.getPath(), range);
        getQTPMap().put(newQtp.getTypeName(), newQtp);
    }

    private void handleRowNumber(SqlKind sqlKind, Object value) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("rowNum value must be of type Integer, but was [" + value.getClass() + "]");
        }
        Integer limit = ((Number) value).intValue();
        if (limit < 0) {
            throw new IllegalArgumentException("rowNum value must be greater than 0");
        }
        switch (sqlKind) {
            case LESS_THAN:
                queryTemplatePacketsHolder.setLimit(limit - 1);
                break;
            case LESS_THAN_OR_EQUAL:
                queryTemplatePacketsHolder.setLimit(limit);
                break;
            default:
                throw new UnsupportedOperationException("rowNum supports less than / less than or equal, but was " +
                        "[" + sqlKind + "]");

        }
    }

    private Object getValue(RexLiteral literal) {
        if (literal == null) {
            return null;
        }
        switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN:
                return RexLiteral.booleanValue(literal);
            case CHAR:
            case VARCHAR:
                return literal.getValueAs(String.class);
            case REAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return literal.getValue3();
            case DATE:
            case TIMESTAMP:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIME:
                return literal.toString(); // we use our parsers with AbstractParser.parse
            default:
                throw new UnsupportedOperationException("Unsupported type: " + literal.getType().getSqlTypeName());
        }
    }

    private QueryTemplatePacket getQueryTemplatePacketForField(RelDataTypeField field) {
        Collection<QueryTemplatePacket> queryTemplatePackets = queryTemplatePacketsHolder.getQueryTemplatePackets().values();
        int totalQueryColumns = 0;
        for (QueryTemplatePacket queryTemplatePacket : queryTemplatePackets) {
            AbstractProjectionTemplate projectionTemplate = queryTemplatePacket.getProjectionTemplate();
            if (projectionTemplate == null) {
                totalQueryColumns += queryTemplatePacket.getTypeDescriptor().getNumOfFixedProperties();
            } else {
                totalQueryColumns += projectionTemplate.getFixedPropertiesIndexes().length;
            }
            if (field.getIndex() < totalQueryColumns) {
                return queryTemplatePacket;
            }
        }
        throw new IllegalStateException("Field [" + field + "] not found in " + queryTemplatePackets.size() + " queryTemplatePackets");
    }

    private void and(WhereHandler leftHandler, WhereHandler rightHandler) {
        //fill qtpMap
        for (Map.Entry<String, QueryTemplatePacket> leftTable : leftHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket rightTable = rightHandler.getQTPMap().get(leftTable.getKey());
            if (rightTable == null) {
                getQTPMap().put(leftTable.getKey(), leftTable.getValue());
            } else if (rightTable instanceof UnionTemplatePacket) {
                getQTPMap().put(leftTable.getKey(), leftTable.getValue().and(((UnionTemplatePacket) rightTable)));
            } else {
                QueryTemplatePacket existingQueryTemplatePacket = getQTPMap().get(leftTable.getKey());
                if (existingQueryTemplatePacket == null) {
                    getQTPMap().put(leftTable.getKey(), leftTable.getValue().and(rightTable));
                } else {
                    getQTPMap().put(leftTable.getKey(), existingQueryTemplatePacket.and(rightTable));
                }
            }
        }

        for (Map.Entry<String, QueryTemplatePacket> rightTable : rightHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket leftTable = leftHandler.getQTPMap().get(rightTable.getKey());
            if (leftTable == null) {
                getQTPMap().put(rightTable.getKey(), rightTable.getValue());
            } else {
                // already handled
            }
        }

    }

    private void or(WhereHandler leftHandler, WhereHandler rightHandler) {
        //fill qtpMap
        for (Map.Entry<String, QueryTemplatePacket> leftTable : leftHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket rightTable = rightHandler.getQTPMap().get(leftTable.getKey());
            if (rightTable == null) {
                getQTPMap().put(leftTable.getKey(), leftTable.getValue());
            } else if (rightTable instanceof UnionTemplatePacket) {
                getQTPMap().put(leftTable.getKey(), leftTable.getValue().union(((UnionTemplatePacket) rightTable)));
            } else {
                QueryTemplatePacket existingQueryTemplatePacket = getQTPMap().get(leftTable.getKey());
                if (existingQueryTemplatePacket == null) {
                    getQTPMap().put(leftTable.getKey(), leftTable.getValue().union(rightTable));
                } else {
                    getQTPMap().put(leftTable.getKey(), existingQueryTemplatePacket.union(rightTable));
                }
            }
        }

        for (Map.Entry<String , QueryTemplatePacket> rightTable : rightHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket leftTable = leftHandler.getQTPMap().get(rightTable.getKey());
            if (leftTable == null) {
                getQTPMap().put(rightTable.getKey(), rightTable.getValue());
            } else {
                // Already handled above
            }
        }
    }

    private RexNode getNode(RexLocalRef localRef) {
        return program.getExprList().get(localRef.getIndex());
    }
}
