package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.result.TableRowUtils;

import java.sql.SQLException;
import java.util.Objects;

public class CaseCondition {

    private final ConditionCode conditionCode;
    private final Object value;
    private final Class<?> valueType;
    private final String fieldName;
    private Object result;


    public CaseCondition(ConditionCode conditionCode, Object value, Class<?> valueType, String fieldName) {
        this.conditionCode = conditionCode;
        this.value = value;
        this.valueType = valueType;
        this.fieldName = fieldName;
    }

    public CaseCondition(ConditionCode conditionCode, Object result) {
        this(conditionCode, null, null, null);
        this.result = result;
    }

    public boolean check(TableRow tableRow) {
        if (conditionCode.equals(ConditionCode.DEFAULT)) {
            return true;
        }
        if (this.fieldName != null && tableRow != null) {
//            Object fieldValue = null;
//            if(Number.class.isAssignableFrom(valueType)) {
//                fieldValue = CalciteUtils.castToNumberType(tableRow.getPropertyValue(this.fieldName), valueType);
//            } else {
//                fieldValue = (tableRow.getPropertyValue(this.fieldName));
//            }
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
            if (first == null) { // nulls smallest by default?
                compareResult = -1;
            } else if (second == null) {
                compareResult = 1;
            } else {
                compareResult = first.compareTo(second);
            }
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
                    throw new UnsupportedOperationException("unsupported condition code");
            }
        }
        return false;
    }


    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Class<?> getValueType() {
        return valueType;
    }

    public enum ConditionCode {
        GT, LT, GE, LE, EQ, NE, DEFAULT
    }

    @Override
    public String toString() {
        if(conditionCode.equals(ConditionCode.DEFAULT)) {
            return conditionCode + " -> " + result;
        }
        return conditionCode + "(" + fieldName + "," + value + ") -> " + result;
    }
}
