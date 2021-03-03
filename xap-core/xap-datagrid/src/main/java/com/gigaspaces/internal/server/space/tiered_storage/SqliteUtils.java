package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.j_spaces.core.client.TemplateMatchCodes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

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
        } else if (propertyType.equals(boolean.class) || propertyType.equals(Boolean.class)) {
            return resultSet.getBoolean(propertyIndex);
        } else if (propertyType.equals(byte.class) || propertyType.equals(Byte.class)) {
            return resultSet.getByte(propertyIndex);
        } else if (propertyType.equals(short.class) || propertyType.equals(Short.class)) {
            return resultSet.getShort(propertyIndex);
        } else if (propertyType.equals(int.class) || propertyType.equals(Integer.class)) {
            return resultSet.getInt(propertyIndex);
        } else if (propertyType.equals(long.class) || propertyType.equals(Long.class)) {
            return resultSet.getLong(propertyIndex);
        } else if (propertyType.equals(BigInteger.class)) {
            return resultSet.getLong(propertyIndex);
        } else if (propertyType.equals(BigDecimal.class)) {
            return resultSet.getBigDecimal(propertyIndex);
        } else if (propertyType.equals(float.class) || propertyType.equals(Float.class)) {
            return resultSet.getFloat(propertyIndex);
        } else if (propertyType.equals(double.class) || propertyType.equals(Double.class)) {
            return resultSet.getDouble(propertyIndex);
        } else if (propertyType.equals(byte[].class) || propertyType.equals(Byte[].class)) {
            return resultSet.getBytes(propertyIndex);
        } else if (propertyType.equals(Timestamp.class)) {
            return resultSet.getTimestamp(propertyIndex);
        } else if (propertyType.equals(Time.class)) {
            return resultSet.getTime(propertyIndex);
        }
        throw new IllegalArgumentException("cannot map non trivial type " + propertyType.getName());
    }

    public static String getValueString(SpacePropertyDescriptor property, Object propertyValue) {
        if(propertyValue == null){
            return "Null";
        } else if (property.getType().equals(String.class)) {
            return "\"" + propertyValue + "\"";
        } else {
            return propertyValue.toString();
        }
    }

    public static String getMatchCodeString(short matchCode) {
        switch (matchCode) {
            case TemplateMatchCodes.EQ:
                return "=";
            case TemplateMatchCodes.GT:
                return ">";
            case TemplateMatchCodes.GE:
                return ">=";
            case TemplateMatchCodes.LT:
                return "<";
            case TemplateMatchCodes.LE:
                return "<=";
            default:
                throw new IllegalStateException("match code " + matchCode + " no supported");
        }
    }
}
