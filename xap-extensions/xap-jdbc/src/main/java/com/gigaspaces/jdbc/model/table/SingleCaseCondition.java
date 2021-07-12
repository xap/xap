package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.result.TableRowUtils;

import java.sql.SQLException;
import java.util.Objects;

public class SingleCaseCondition implements ICaseCondition{

    private final ConditionCode conditionCode;
    private final Object value;
    private final Class<?> valueType;
    private final String fieldName;
    private Object result;


    public SingleCaseCondition(ConditionCode conditionCode, Object value, Class<?> valueType, String fieldName) {
        this.conditionCode = conditionCode;
        this.value = value;
        this.valueType = valueType;
        this.fieldName = fieldName;
    }

    public SingleCaseCondition(ConditionCode conditionCode, Object result) {
        this(conditionCode, null, null, null);
        this.result = result;
    }

    @Override
    public boolean check(TableRow tableRow) {
        if (conditionCode.equals(ConditionCode.DEFAULT_TRUE)) {
            return true;
        }else if (conditionCode.equals(ConditionCode.DEFAULT_FALSE)) {
            return false;
        }
        if (this.fieldName != null && tableRow != null) {
            Object fieldValue = null;
            try {
                fieldValue = ObjectConverter.convert(tableRow.getPropertyValue(this.fieldName), valueType);
            } catch (SQLException e) {
                throw new SQLExceptionWrapper(e);
            }
            ;
            Comparable first = TableRowUtils.castToComparable(fieldValue);
            Comparable second = TableRowUtils.castToComparable(value);
            int compareResult;
            if (first == null || second == null) {
                return false;
            }
            compareResult = first.compareTo(second);
            switch (conditionCode) {
                case EQ:
                    return Objects.equals(value, fieldValue);
                case NE:
                    return !Objects.equals(value, fieldValue);
                case LE:
                    return compareResult <= 0;
                case LT:
                    return compareResult < 0;
                case GE:
                    return compareResult >= 0;
                case GT:
                    return compareResult > 0;
                default:
                    throw new UnsupportedOperationException("unsupported condition code: " + conditionCode);
            }
        }
        return false;
    }

    @Override
    public Object getResult() {
        return result;
    }

    @Override
    public void setResult(Object result) {
        this.result = result;
    }

    public enum ConditionCode {
        GT, LT, GE, LE, EQ, NE, DEFAULT_TRUE, DEFAULT_FALSE
    }

    @Override
    public String toString() {
        if(conditionCode.equals(ConditionCode.DEFAULT_TRUE) || conditionCode.equals(ConditionCode.DEFAULT_FALSE)) {
            return conditionCode + " -> " + result;
        }
        return conditionCode + "(" + fieldName + "," + value + ") -> " + result;
    }
}
