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
package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.AbstractCompundCustomQuery;
import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.range.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

public class SqliteUtils {

    public static String getPropertyType(PropertyInfo property) {
        Class<?> propertyType = property.getType();
        if (propertyType.equals(String.class)) {
            return "VARCHAR";
        } else if (propertyType.equals(boolean.class) || propertyType.equals(Boolean.class)) {
            return "BIT";
        } else if (propertyType.equals(byte.class) || propertyType.equals(Byte.class)) {
            return "TINYINT";
        } else if (propertyType.equals(short.class) || propertyType.equals(Short.class)) {
            return "SMALLINT";
        } else if (propertyType.equals(int.class) || propertyType.equals(Integer.class)) {
            return "INTEGER";
        } else if (propertyType.equals(long.class) || propertyType.equals(Long.class)) {
            return "BIGINT";
        } else if (propertyType.equals(BigInteger.class)) {
            return "BIGINT";
        } else if (propertyType.equals(BigDecimal.class)) {
            return "DECIMAL";
        } else if (propertyType.equals(float.class) || propertyType.equals(Float.class)) {
            return "REAL";
        } else if (propertyType.equals(double.class) || propertyType.equals(Double.class)) {
            return "float";
        } else if (propertyType.equals(byte[].class) || propertyType.equals(Byte[].class)) {
            return "BINARY";
        } else if (propertyType.equals(Timestamp.class)) {
            return "DATETIME";
        } else if (propertyType.equals(Time.class)) {
            return "BIGTIME";
        }
        throw new IllegalArgumentException("cannot map non trivial type " + propertyType.getName());
    }

    public static Object getPropertyValue(ResultSet resultSet, PropertyInfo property) throws SQLException {
        Class<?> propertyType = property.getType();
        int propertyIndex = property.getOriginalIndex() + 1;
        if (propertyType.equals(String.class)) {
            return resultSet.getString(propertyIndex);
        } else if (propertyType.equals(boolean.class)) {
            return resultSet.getBoolean(propertyIndex);
        } else if (propertyType.equals(Boolean.class)) {
            return resultSet.getObject(propertyIndex);
        } else if (propertyType.equals(byte.class)) {
            return resultSet.getByte(propertyIndex);
        } else if (propertyType.equals(Byte.class)) {
            return resultSet.getObject(propertyIndex);
        } else if (propertyType.equals(short.class)) {
            return resultSet.getShort(propertyIndex);
        } else if (propertyType.equals(Short.class)) {
            return resultSet.getObject(propertyIndex);
        } else if (propertyType.equals(int.class)) {
            return resultSet.getInt(propertyIndex);
        } else if (propertyType.equals(Integer.class)) {
            return resultSet.getObject(propertyIndex);
        } else if (propertyType.equals(long.class)) {
            return resultSet.getLong(propertyIndex);
        } else if (propertyType.equals(Long.class)) {
            return resultSet.getObject(propertyIndex);
        } else if (propertyType.equals(BigInteger.class)) {
            return resultSet.getLong(propertyIndex);
        } else if (propertyType.equals(BigDecimal.class)) {
            return resultSet.getBigDecimal(propertyIndex);
        } else if (propertyType.equals(float.class)) {
            return resultSet.getFloat(propertyIndex);
        } else if (propertyType.equals(Float.class)) {
            return resultSet.getObject(propertyIndex);
        } else if (propertyType.equals(double.class)) {
            return resultSet.getDouble(propertyIndex);
        } else if (propertyType.equals(Double.class)) {
            return resultSet.getObject(propertyIndex);
        } else if (propertyType.equals(byte[].class) || propertyType.equals(Byte[].class)) {
            return resultSet.getBytes(propertyIndex);
        } else if (propertyType.equals(Timestamp.class)) {
            return resultSet.getTimestamp(propertyIndex);
        } else if (propertyType.equals(Time.class)) {
            return resultSet.getTime(propertyIndex);
        }
        throw new IllegalArgumentException("cannot map non trivial type " + propertyType.getName());
    }

    public static String getValueString(Object propertyValue) {
        if (propertyValue == null) {
            return "Null";
        } else if (propertyValue.getClass().equals(String.class)) {
            return "\"" + propertyValue + "\"";
        } else {
            return propertyValue.toString();
        }
    }

    public static String getMatchCodeString(short matchCode) {
        switch (matchCode) {
            case TemplateMatchCodes.EQ:
                return " = ";
            case TemplateMatchCodes.GT:
                return " > ";
            case TemplateMatchCodes.GE:
                return " >= ";
            case TemplateMatchCodes.LT:
                return " < ";
            case TemplateMatchCodes.LE:
                return " <= ";
            case TemplateMatchCodes.NE:
                return " != ";
            default:
                throw new IllegalStateException("match code " + matchCode + " no supported");
        }
    }

    public static String getMatchCodeString(short originalMatchCode, boolean inclusion) {
        switch (originalMatchCode) {
            case TemplateMatchCodes.GT:
            case TemplateMatchCodes.GE:
                return inclusion ? " <= " : " < " ;
            case TemplateMatchCodes.LT:
            case TemplateMatchCodes.LE:
                return inclusion ? " >= " : " > " ;
            default:
                throw new IllegalStateException("match code " + originalMatchCode + " no supported");
        }
    }

    public static String getRangeString(Range range){
        StringBuilder stringBuilder = new StringBuilder(range.getPath());
        if (range.isEqualValueRange()) {
            stringBuilder.append(" = ").append(getValueString(((EqualValueRange) range).getValue()));
        } else if (range instanceof NotEqualValueRange){
            stringBuilder.append(" != ").append(getValueString(((NotEqualValueRange) range).getValue()));
        } else if (range.isSegmentRange()){
            SegmentRange segmentRange = (SegmentRange) range;
            Comparable min = segmentRange.getMin();
            Comparable max = segmentRange.getMax();
            String includeMinSign = segmentRange.isIncludeMin() ? "= ": " ";
            String includeMaxSign = segmentRange.isIncludeMax() ? "= ": " ";
            if (min != null && max == null){
                stringBuilder.append(" >").append(includeMinSign).append(min);
            } else if (min == null && max != null){
                stringBuilder.append(" <").append(includeMaxSign).append(max);
            }  else { // max != null && min != null
                stringBuilder.append(" <").append(includeMaxSign).append(max).append(" AND ")
                        .append(range.getPath()).append(" >").append(includeMinSign).append(min);
            }
        } else if (range instanceof InRange) {
            InRange inRange = (InRange) range;
            stringBuilder.append(" IN(");
            for (Object val : inRange.getInValues()) {
                stringBuilder.append(getValueString(val)).append(",");
            }
            int lastIndexOf = stringBuilder.lastIndexOf(",");
            if (lastIndexOf != -1){
                stringBuilder.deleteCharAt(lastIndexOf);
            }
            stringBuilder.append(")");
        } else if (range instanceof IsNullRange){
            stringBuilder.append(" IS NULL");
        } else if (range instanceof NotNullRange){
            stringBuilder.append(" IS NOT NULL");
        } else {
            throw new IllegalStateException("SQL query of type" + range.getClass().toString() + " is unsupported");
        }
        return stringBuilder.toString();
    }

    public static String getCustomQueryString(ICustomQuery customQuery) {
        if (customQuery instanceof AbstractCompundCustomQuery) {
            String operation;
            if (customQuery.getClass().equals(CompoundAndCustomQuery.class)) {
                operation = " AND ";
            } else {
                operation = " OR ";
            }
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery)customQuery).get_subQueries();
            StringBuilder stringBuilder = new StringBuilder();
            for (ICustomQuery query :subQueries){
                stringBuilder.append(getCustomQueryString(query)).append(operation);
            }
            int lastIndexOf = stringBuilder.lastIndexOf(operation);
            if (lastIndexOf != -1) {
                stringBuilder.delete(lastIndexOf, stringBuilder.length());
            }
            return stringBuilder.toString();
        } else if (customQuery instanceof Range) {
            return SqliteUtils.getRangeString((Range)customQuery);
        } else {
            throw new IllegalArgumentException("SQL query of type" + customQuery.getClass().toString() + " is unsupported");
        }
    }
}
