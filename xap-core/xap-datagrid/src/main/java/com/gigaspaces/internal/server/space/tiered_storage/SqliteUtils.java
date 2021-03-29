package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.AbstractCompundCustomQuery;
import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.TemplateEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.*;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public class SqliteUtils {
    private static final long NANOS_PER_SEC = 1_000_000_000;
    private static final long OFFSET = 0;

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
        } else if (propertyType.equals(Instant.class)) { //is converted to special long which includes millis and nanos
            return "BIGINT";
        } else if (propertyType.equals(Timestamp.class)) {  //is converted to special long which includes millis and nanos
            return "BIGINT";
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
        } else if (propertyType.equals(Instant.class)) {
            return fromGsTime(resultSet.getLong(propertyIndex));
        } else if (propertyType.equals(Timestamp.class)) {
            Instant instant = fromGsTime(resultSet.getLong(propertyIndex));
            return Timestamp.from(instant);
        }
        throw new IllegalArgumentException("cannot map non trivial type " + propertyType.getName());
    }

    public static Object getValueForSqlExpression(Object propertyValue) {
        if (propertyValue == null) {
            return "Null";
        } else if (propertyValue.getClass().equals(String.class)) {
            return "\"" + propertyValue + "\"";
        } else if (propertyValue.getClass().equals(Instant.class)) {
            return toGSTime((Instant)propertyValue);
        } else if (propertyValue.getClass().equals(Timestamp.class)){
            Instant instant = ((Timestamp) propertyValue).toInstant();
            return toGSTime((instant));
        } else {
            return propertyValue.toString();
        }
    }

   public static long toGSTime(Instant instant) { // long which consists 2 parts: 1. time in milliseconds 2. time in nanos
        return (instant.getEpochSecond() - OFFSET) * NANOS_PER_SEC + instant.getNano();  //
    }
    public static Instant fromGsTime(long l) {
        long secs =  l / NANOS_PER_SEC;
        int nanos = (int)(l % NANOS_PER_SEC);
        return Instant.ofEpochSecond(secs + OFFSET, nanos);
    }

    private static String gsTimeToString(Instant instant) {
        return instant.toString() + " [" + instant.getEpochSecond() + "|" + instant.getNano() + "]";
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
            stringBuilder.append(" = ").append(getValueForSqlExpression(((EqualValueRange) range).getValue()));
        } else if (range instanceof NotEqualValueRange){
            stringBuilder.append(" != ").append(getValueForSqlExpression(((NotEqualValueRange) range).getValue()));
        } else if (range.isSegmentRange()){
            SegmentRange segmentRange = (SegmentRange) range;
            Comparable min = segmentRange.getMin();
            Comparable max = segmentRange.getMax();
            String includeMinSign = segmentRange.isIncludeMin() ? "= ": " ";
            String includeMaxSign = segmentRange.isIncludeMax() ? "= ": " ";
            if (min != null && max == null){
                stringBuilder.append(" >").append(includeMinSign).append(getValueForSqlExpression(min));
            } else if (min == null && max != null){
                stringBuilder.append(" <").append(includeMaxSign).append(getValueForSqlExpression(max));
            }  else { // max != null && min != null
                stringBuilder.append(" <").append(includeMaxSign).append(getValueForSqlExpression(max)).append(" AND ")
                        .append(range.getPath()).append(" >").append(includeMinSign).append(getValueForSqlExpression(min));
            }
        } else if (range instanceof InRange) {
            InRange inRange = (InRange) range;
            stringBuilder.append(" IN(");
            for (Object val : inRange.getInValues()) {
                stringBuilder.append(getValueForSqlExpression(val)).append(",");
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
            throw new IllegalStateException("SQL query of type" + customQuery.getClass().toString() + " is unsupported");
        }
    }

    public static TemplateMatchTier evaluateByMatchTier(ITemplateHolder template, TemplateMatchTier templateMatchTier){
        if (templateMatchTier == TemplateMatchTier.MATCH_HOT_AND_COLD) {
            return template.isMemoryOnlySearch() ? TemplateMatchTier.MATCH_HOT :
                    (template.getBatchOperationContext() != null && template.getBatchOperationContext().getMaxEntries() < Integer.MAX_VALUE ?
                            templateMatchTier : TemplateMatchTier.MATCH_COLD);
        }
        return templateMatchTier;
    }

    public static TemplateMatchTier getTemplateMatchTier(Range criteria, ITemplateHolder template, String timeType) {
        if (template.getCustomQuery() != null) {
            ICustomQuery customQuery = template.getCustomQuery();
            return evalCustomQuery(criteria, customQuery, timeType);
        } else {
            ITypeDesc typeDesc = template.getServerTypeDesc().getTypeDesc();
            int index = ((PropertyInfo) typeDesc.getFixedProperty(criteria.getPath())).getOriginalIndex();
            TemplateEntryData entryData = template.getTemplateEntryData();
            Object value = entryData.getFixedPropertyValue(index);

            if (template.getExtendedMatchCodes() == null || (template.isIdQuery() && typeDesc.getIdPropertyName().equalsIgnoreCase(criteria.getPath()))) {
                if (value != null && timeType != null){
                    value = convertTimeTypeToInstant(value);
                }
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : (criteria.getPredicate().execute(value) ? TemplateMatchTier.MATCH_HOT : TemplateMatchTier.MATCH_COLD);
            } else if (template.getExtendedMatchCodes() != null) {
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : evalRange(criteria, getRangeFromMatchCode(entryData, value, index, criteria.getPath()), timeType);
            }
        }
        return TemplateMatchTier.MATCH_HOT_AND_COLD;
    }

    public static TemplateMatchTier getTemplateMatchTier(Range criteria, ITemplatePacket packet, String timeType) {
        if (packet.getCustomQuery() != null) {
            ICustomQuery customQuery = packet.getCustomQuery();
            return evalCustomQuery(criteria, customQuery, timeType);
        } else {
            SpacePropertyDescriptor property = packet.getTypeDescriptor().getFixedProperty(criteria.getPath());
            if (property == null) {
                throw new IllegalStateException("type " + packet.getTypeDescriptor().getTypeName() + " reached CachePredicate.evaluate but has no property " + criteria.getPath());
            }
            int index = ((PropertyInfo) property).getOriginalIndex();
            Object value = packet.getFieldValue(index);
            if (packet instanceof TemplatePacket || (packet.isIdQuery() && packet.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(criteria.getPath()))) {
                if (value != null && timeType != null){
                    value = convertTimeTypeToInstant(value);
                }
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : (criteria.getPredicate().execute(value) ? TemplateMatchTier.MATCH_HOT : TemplateMatchTier.MATCH_COLD);
            } else if (hasMatchCodes(packet)) {
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : SqliteUtils.evalRange(criteria, ((QueryTemplatePacket) packet).getRanges().get(criteria.getPath()), timeType); //todo
            }
        }
        return TemplateMatchTier.MATCH_HOT_AND_COLD;
    }

    public static Object convertTimeTypeToInstant(Object value) {
        if (Instant.class.equals(value.getClass())){
            return value;
        } else if (Timestamp.class.equals(value.getClass())){
            return ((Timestamp) value).toInstant();
        } else if (Long.class.equals(value.getClass())){
            return Instant.ofEpochSecond((long)value);
        }
        throw new IllegalStateException("Time type of " + value.getClass().toString() + " is unsupported");
    }

    private static Range convertRangeWithTimestampToInstant(Range queryValueRange){
        if (queryValueRange.isEqualValueRange()){
            Timestamp timestamp = (Timestamp)((EqualValueRange)queryValueRange).getValue();
            return new EqualValueRange(queryValueRange.getPath(), timestamp.toInstant());
        } else if (queryValueRange.isSegmentRange()){
            SegmentRange segmentRange = (SegmentRange) queryValueRange;
            Comparable<Instant> minInstant = segmentRange.getMin() != null? ((Timestamp)segmentRange.getMin()).toInstant() : null;
            Comparable<Instant> maxInstant = segmentRange.getMax() != null? ((Timestamp)segmentRange.getMax()).toInstant() : null;
            return new SegmentRange(queryValueRange.getPath(), minInstant, ((SegmentRange) queryValueRange).isIncludeMin(), maxInstant, ((SegmentRange) queryValueRange).isIncludeMax());
        }
        throw new IllegalStateException("SQL query of type" + queryValueRange.getClass().toString() + " is unsupported"); //todo= e.g. InRange
    }

    private static Range convertRangeWithTimeAsLongToInstant(Range queryValueRange){
        if (queryValueRange.isEqualValueRange()){
            long value = (long)((EqualValueRange)queryValueRange).getValue();
            return new EqualValueRange(queryValueRange.getPath(), Instant.ofEpochSecond(value));
        } else if (queryValueRange.isSegmentRange()){
            SegmentRange segmentRange = (SegmentRange) queryValueRange;
            Comparable<Instant> minInstant = segmentRange.getMin() != null? Instant.ofEpochSecond((long)segmentRange.getMin()) : null;
            Comparable<Instant> maxInstant = segmentRange.getMax() != null? Instant.ofEpochSecond((long)segmentRange.getMax()) : null;
            return new SegmentRange(queryValueRange.getPath(), minInstant, ((SegmentRange) queryValueRange).isIncludeMin(), maxInstant, ((SegmentRange) queryValueRange).isIncludeMax());
        }
        throw new IllegalStateException("SQL query of type" + queryValueRange.getClass().toString() + " is unsupported"); //todo= e.g. InRange
    }


    public static TemplateMatchTier evalRange(Range criteria, Range queryValueRange, String timeType) {
        if (Timestamp.class.getName().equals(timeType)){
            queryValueRange = convertRangeWithTimestampToInstant(queryValueRange);
        } else if (Long.class.getName().equals(timeType) || long.class.getName().equals(timeType)){
            queryValueRange = convertRangeWithTimeAsLongToInstant(queryValueRange);
        }
        if (queryValueRange.isEqualValueRange()) {
            return getTemplateMatchTierForEqualQuery(criteria, (EqualValueRange) queryValueRange);
        } else if (queryValueRange instanceof InRange) {
            return getTemplateMatchTierForInQuery(criteria, (InRange) queryValueRange);

        } else if (criteria.isEqualValueRange()) {
            Object criteriaValue = ((EqualValueRange) criteria).getValue();
            if (queryValueRange.isSegmentRange()) {
                SegmentRange querySegmentRange = (SegmentRange) queryValueRange;
                if (querySegmentRange.getMin() != null && querySegmentRange.getMin().equals(criteriaValue) && querySegmentRange.isIncludeMin()
                        && querySegmentRange.getMax() != null && querySegmentRange.getMax().equals(criteriaValue) && querySegmentRange.isIncludeMax()) {
                    return TemplateMatchTier.MATCH_HOT;
                } else if (queryValueRange.getPredicate().execute(criteriaValue)) {
                    return TemplateMatchTier.MATCH_HOT_AND_COLD;
                } else {
                    return TemplateMatchTier.MATCH_COLD;
                }
            }
        } else if (criteria.isSegmentRange()) {
            Range intersection = criteria.intersection(queryValueRange);
            if (intersection == Range.EMPTY_RANGE) {
                return TemplateMatchTier.MATCH_COLD;
            } else {
                SegmentRange querySegmentRange = (SegmentRange) queryValueRange;
                SegmentRange criteriaSegmentRange = (SegmentRange) criteria;
                if (querySegmentRange.getMin() != null && querySegmentRange.getMax() != null &&
                        criteria.getPredicate().execute(querySegmentRange.getMax()) && criteria.getPredicate().execute(querySegmentRange.getMin())) {
                    return TemplateMatchTier.MATCH_HOT;
                } else if (criteriaSegmentRange.getMin() != null && criteriaSegmentRange.getMax() == null
                        && querySegmentRange.getMin() != null && querySegmentRange.getMax() == null && criteria.getPredicate().execute(querySegmentRange.getMin())) {
                    return TemplateMatchTier.MATCH_HOT;
                }
                if (criteriaSegmentRange.getMin() == null && criteriaSegmentRange.getMax() != null
                        && querySegmentRange.getMin() == null && querySegmentRange.getMax() != null && criteria.getPredicate().execute(querySegmentRange.getMax())) {
                    return TemplateMatchTier.MATCH_HOT;
                } else {
                    return TemplateMatchTier.MATCH_HOT_AND_COLD;
                }
            }
        } else if (criteria instanceof InRange) {
            if (queryValueRange.isSegmentRange()) {
                InRange criteriaInRange = (InRange) criteria;
                boolean noneMatched = false;
                for (Object criteriaInRangeInValue : criteriaInRange.getInValues()) {
                    boolean match = queryValueRange.getPredicate().execute(criteriaInRangeInValue);
                    noneMatched |= match;
                }
                return noneMatched ? TemplateMatchTier.MATCH_COLD : TemplateMatchTier.MATCH_HOT_AND_COLD;
            }
        }
        return TemplateMatchTier.MATCH_COLD;

    }

    public static TemplateMatchTier evalCustomQuery(Range criteria, ICustomQuery customQuery, String timeType) {
        if (customQuery instanceof CompoundAndCustomQuery) {
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery) customQuery).get_subQueries();
            TemplateMatchTier result = TemplateMatchTier.MATCH_COLD;
            for (ICustomQuery query : subQueries) {
                result = andTemplateMatchTier(result, evalCustomQuery(criteria, query, timeType));
            }
            return result;
        } else if (customQuery instanceof CompoundOrCustomQuery) {
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery) customQuery).get_subQueries();
            TemplateMatchTier result = TemplateMatchTier.MATCH_HOT;
            for (ICustomQuery query : subQueries) {
                result = orTemplateMatchTier(result, evalCustomQuery(criteria, query, timeType));
            }
            return result;
        } else if (customQuery instanceof Range) {
            Range queryValueRange = (Range) customQuery;
            if (queryValueRange.getPath().equalsIgnoreCase(criteria.getPath())) {
                return evalRange(criteria, queryValueRange, timeType);
            } else {
                return TemplateMatchTier.MATCH_HOT_AND_COLD;
            }
        }
        return TemplateMatchTier.MATCH_COLD;
    }

    private static TemplateMatchTier andTemplateMatchTier(TemplateMatchTier tier1, TemplateMatchTier tier2) {
        if (tier1 == TemplateMatchTier.MATCH_HOT || tier2 == TemplateMatchTier.MATCH_HOT) {
            return TemplateMatchTier.MATCH_HOT;
        } else if (tier1 == TemplateMatchTier.MATCH_COLD && tier2 == TemplateMatchTier.MATCH_COLD) {
            return TemplateMatchTier.MATCH_COLD;
        } else {
            return TemplateMatchTier.MATCH_HOT_AND_COLD;
        }
    }

    private static TemplateMatchTier orTemplateMatchTier(TemplateMatchTier tier1, TemplateMatchTier tier2) {
        if (tier1 == TemplateMatchTier.MATCH_HOT && tier2 == TemplateMatchTier.MATCH_HOT) {
            return TemplateMatchTier.MATCH_HOT;
        } else if (tier1 == TemplateMatchTier.MATCH_COLD && tier2 == TemplateMatchTier.MATCH_COLD) {
            return TemplateMatchTier.MATCH_COLD;
        } else {
            return TemplateMatchTier.MATCH_HOT_AND_COLD;
        }
    }

    private static TemplateMatchTier getTemplateMatchTierForEqualQuery(Range criteria, EqualValueRange queryValueRange) {
        if (criteria.getPredicate().execute(queryValueRange.getValue())) {
            return TemplateMatchTier.MATCH_HOT;
        } else {
            return TemplateMatchTier.MATCH_COLD;
        }
    }

    private static TemplateMatchTier getTemplateMatchTierForInQuery(Range criteria, InRange queryValueRange) {
        if (criteria.isEqualValueRange()) {
            Object criteriaValue = ((EqualValueRange) criteria).getValue();
            if (queryValueRange.getInValues().size() == 1 && queryValueRange.getInValues().iterator().next().equals(criteriaValue)) {
                return TemplateMatchTier.MATCH_HOT;
            } else if (queryValueRange.getPredicate().execute(criteriaValue)) {
                return TemplateMatchTier.MATCH_HOT_AND_COLD;
            } else {
                return TemplateMatchTier.MATCH_COLD;
            }
        } else {
            boolean allMatched = true;
            boolean noneMatched = false;
            for (Object queryInRangeInValue : queryValueRange.getInValues()) {
                boolean match = criteria.getPredicate().execute(queryInRangeInValue);
                allMatched &= match;
                noneMatched |= match;
            }

            return allMatched ? TemplateMatchTier.MATCH_HOT :
                    (noneMatched ? TemplateMatchTier.MATCH_COLD : TemplateMatchTier.MATCH_HOT_AND_COLD);
        }
    }


    public static Range getRangeFromMatchCode(TemplateEntryData entryData, Object value, int index, String path) {
        short matchCode = entryData.getExtendedMatchCodes()[index];
        switch (matchCode) {
            case TemplateMatchCodes.EQ:
                return new EqualValueRange(path, value);
            case TemplateMatchCodes.NE:
                return new NotEqualValueRange(path, value);
            case TemplateMatchCodes.LT:
                return entryData.getRangeValue(index) != null
                        ? new SegmentRange(path, (Comparable<?>) entryData.getRangeValue(index), entryData.getRangeInclusion(index), (Comparable<?>) value, false)
                        : new SegmentRange(path, null, true, (Comparable<?>) value, false);
            case TemplateMatchCodes.LE:
                return entryData.getRangeValue(index) != null
                        ? new SegmentRange(path, (Comparable<?>) entryData.getRangeValue(index), entryData.getRangeInclusion(index), (Comparable<?>) value, false)
                        : new SegmentRange(path, null, true, (Comparable<?>) value, true);
            case TemplateMatchCodes.GT:
                return entryData.getRangeValue(index) != null
                        ? new SegmentRange(path, (Comparable<?>) value, false, (Comparable<?>) entryData.getRangeValue(index), entryData.getRangeInclusion(index))
                        : new SegmentRange(path, (Comparable<?>) value, false, null, false);
            case TemplateMatchCodes.GE:
                return entryData.getRangeValue(index) != null
                        ? new SegmentRange(path, (Comparable<?>) value, false, (Comparable<?>) entryData.getRangeValue(index), entryData.getRangeInclusion(index))
                        : new SegmentRange(path, (Comparable<?>) value, true, null, false);
            default:
                throw new IllegalArgumentException("match codes with code " + matchCode);
        }
    }


    private static boolean hasMatchCodes(ITemplatePacket packet) {
        Object[] fieldValues = packet.getFieldValues();
        for (Object fieldValue : fieldValues) {
            if (fieldValue != null)
                return true;
        }
        return false;
    }
}
