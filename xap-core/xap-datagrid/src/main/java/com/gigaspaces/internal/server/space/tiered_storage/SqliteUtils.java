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
import java.sql.*;
import java.time.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class SqliteUtils {
    private static final long NANOS_PER_SEC = 1_000_000_000;
    private static final long OFFSET = 0;
    private static Map<String, String> sqlTypesMap = initSqlTypesMap();
    private static Map<String, ExtractFunction> sqlExtractorsMap = initSqlExtractorsMap();
    private static Map<String, InjectFunction> sqlInjectorsMap = initSqlInjectorsMap();
    private static Map<String, RangeToStringFunction> rangeToStringFunctionMap = initRangeToStringFunctionMap();

    private static Map<String, RangeToStringFunction> initRangeToStringFunctionMap() {
        Map<String, RangeToStringFunction> map = new HashMap<>();
        //Is null range
        map.put(IsNullRange.class.getName(), (range, queryBuilder, queryParams) -> queryBuilder.append(range.getPath()).append(" IS NULL"));

        //Is not null range
        map.put(NotNullRange.class.getName(), (range, queryBuilder, queryParams) -> queryBuilder.append(range.getPath()).append(" IS NOT NULL"));

        //Equal range
        map.put(EqualValueRange.class.getName(), (range, queryBuilder, queryParams) -> {
            queryBuilder.append(range.getPath()).append(" = ?");
            queryParams.addParameter(range.getPath(), ((EqualValueRange) range).getValue());
        });

        //Not equal range
        map.put(NotEqualValueRange.class.getName(), (range, queryBuilder, queryParams) -> {
            queryBuilder.append(range.getPath()).append(" != ?");
            queryParams.addParameter(range.getPath(), ((NotEqualValueRange) range).getValue());
        });

        //In range
        map.put(InRange.class.getName(), (range, queryBuilder, queryParams) -> {
            InRange inRange = (InRange) range;
            queryBuilder.append(range.getPath()).append(" IN(");
            StringJoiner stringValues = new StringJoiner(", ");
            for (Object val : inRange.getInValues()) {
                stringValues.add("?");
                queryParams.addParameter(range.getPath(), val);
            }
            queryBuilder.append(stringValues);
            queryBuilder.append(")");
        });

        //Segment range
        map.put(SegmentRange.class.getName(), (range, queryBuilder, queryParams) -> {
            SegmentRange segmentRange = (SegmentRange) range;
            String path = range.getPath();
            Comparable min = segmentRange.getMin();
            Comparable max = segmentRange.getMax();
            String includeMinSign = segmentRange.isIncludeMin() ? "= " : " ";
            String includeMaxSign = segmentRange.isIncludeMax() ? "= " : " ";
            queryBuilder.append(range.getPath());
            if (min != null && max == null) {
                queryBuilder.append(" > ?");
                queryParams.addParameter(path, min);
            } else if (min == null && max != null) {
                queryBuilder.append(" <").append(includeMaxSign).append("?");
                queryParams.addParameter(path, max);
            } else { // max != null && min != null
                queryBuilder.append(" <").append(includeMaxSign).append("? AND ")
                        .append(path).append(" >").append(includeMinSign).append("?");
                queryParams.addParameter(path, max);
                queryParams.addParameter(path, min);
            }
        });

        return map;
    }

    private static Map<String, String> initSqlTypesMap() {
        Map<String, String> map = new HashMap<>();
        map.put(String.class.getName(), "VARCHAR");
        map.put(boolean.class.getName(), "BIT");
        map.put(Boolean.class.getName(), "BIT");
        map.put(byte.class.getName(), "TINYINT");
        map.put(Byte.class.getName(), "TINYINT");
        map.put(short.class.getName(), "SMALLINT");
        map.put(Short.class.getName(), "SMALLINT");
        map.put(int.class.getName(), "INTEGER");
        map.put(Integer.class.getName(), "INTEGER");
        map.put(long.class.getName(), "BIGINT");
        map.put(Long.class.getName(), "BIGINT");
        map.put(BigInteger.class.getName(), "BIGINT");
        map.put(BigDecimal.class.getName(), "DECIMAL");
        map.put(float.class.getName(), "REAL");
        map.put(Float.class.getName(), "REAL");
        map.put(double.class.getName(), "FLOAT");
        map.put(Double.class.getName(), "FLOAT");
        map.put(byte[].class.getName(), "BINARY");
        map.put(Byte[].class.getName(), "BINARY");
        map.put(Instant.class.getName(), "BIGINT");
        map.put(Timestamp.class.getName(), "BIGINT");
        map.put(Date.class.getName(), "BIGINT");
        map.put(java.sql.Date.class.getName(), "BIGINT");
        map.put(java.sql.Time.class.getName(), "BIGINT");
        map.put(LocalDate.class.getName(), "BIGINT");
        map.put(LocalTime.class.getName(), "BIGINT");
        map.put(LocalDateTime.class.getName(), "BIGINT");
        return map;
    }

    private static Map<String, ExtractFunction> initSqlExtractorsMap() {
        Map<String, ExtractFunction> map = new HashMap<>();
        map.put(String.class.getName(), ResultSet::getString);
        map.put(boolean.class.getName(), ResultSet::getBoolean);
        map.put(Boolean.class.getName(), ResultSet::getBoolean);
        map.put(byte.class.getName(), ResultSet::getByte);
        map.put(Byte.class.getName(), ResultSet::getByte);
        map.put(short.class.getName(), ResultSet::getShort);
        map.put(Short.class.getName(), ResultSet::getShort);
        map.put(int.class.getName(), ResultSet::getInt);
        map.put(Integer.class.getName(), ResultSet::getInt);
        map.put(long.class.getName(), ResultSet::getLong);
        map.put(Long.class.getName(), ResultSet::getLong);
        map.put(BigInteger.class.getName(), ResultSet::getLong); // todo - check
        map.put(BigDecimal.class.getName(), ResultSet::getBigDecimal);
        map.put(float.class.getName(), ResultSet::getFloat);
        map.put(Float.class.getName(), ResultSet::getFloat);
        map.put(double.class.getName(), ResultSet::getDouble);
        map.put(Double.class.getName(), ResultSet::getDouble);
        map.put(byte[].class.getName(), ResultSet::getBytes);
        map.put(Byte[].class.getName(), ResultSet::getBytes);
        map.put(Instant.class.getName(), (res, i) -> fromGsTime(res.getLong(i)));
        map.put(Timestamp.class.getName(), (res, i) -> Timestamp.from(fromGsTime(res.getLong(i))));
        map.put(Date.class.getName(), (res, i) -> new Date(res.getLong(i)));
        map.put(java.sql.Date.class.getName(), (res, i) -> new java.sql.Date(res.getLong(i)));
        map.put(java.sql.Time.class.getName(), (res, i) -> new Time(res.getLong(i)));
        map.put(LocalDate.class.getName(), (res, i) -> LocalDate.ofEpochDay(res.getLong(i)));
        map.put(LocalTime.class.getName(), (res, i) -> LocalTime.ofNanoOfDay(res.getLong(i)));
        map.put(LocalDateTime.class.getName(), (res, i) -> LocalDateTime.ofInstant(fromGsTime(res.getLong(i)), ZoneId.of("UTC")));
        return map;
    }

    private static Map<String, InjectFunction> initSqlInjectorsMap() {
        Map<String, InjectFunction> map = new HashMap<>();
        map.put(String.class.getName(), (statement, i, val) -> statement.setString(i, (String) val));
        map.put(boolean.class.getName(), (statement, i, val) -> statement.setBoolean(i, (boolean) val));
        map.put(Boolean.class.getName(), (statement, i, val) -> statement.setBoolean(i, (Boolean) val));
        map.put(byte.class.getName(), (statement, i, val) -> statement.setByte(i, (byte) val));
        map.put(Byte.class.getName(), (statement, i, val) -> statement.setByte(i, (Byte) val));
        map.put(short.class.getName(), (statement, i, val) -> statement.setShort(i, (short) val));
        map.put(Short.class.getName(), (statement, i, val) -> statement.setShort(i, (Short) val));
        map.put(int.class.getName(), (statement, i, val) -> statement.setInt(i, (int) val));
        map.put(Integer.class.getName(), (statement, i, val) -> statement.setInt(i, (Integer) val));
        map.put(long.class.getName(), (statement, i, val) -> statement.setLong(i, (long) val));
        map.put(Long.class.getName(), (statement, i, val) -> statement.setLong(i, (Long) val));
        map.put(BigInteger.class.getName(), (statement, i, val) -> statement.setLong(i, (Long) val));//todo - check
        map.put(BigDecimal.class.getName(), (statement, i, val) -> statement.setBigDecimal(i, (BigDecimal) val));
        map.put(float.class.getName(), (statement, i, val) -> statement.setFloat(i, (float) val));
        map.put(Float.class.getName(), (statement, i, val) -> statement.setFloat(i, (Float) val));
        map.put(double.class.getName(), (statement, i, val) -> statement.setDouble(i, (double) val));
        map.put(Double.class.getName(), (statement, i, val) -> statement.setDouble(i, (Double) val));
        map.put(byte[].class.getName(), (statement, i, val) -> statement.setBytes(i, (byte[]) val));
        map.put(Byte[].class.getName(), (statement, i, val) -> statement.setBytes(i, (byte[]) val));
        map.put(Instant.class.getName(), (statement, i, val) -> statement.setLong(i, toGSTime(((Instant) val))));
        map.put(Timestamp.class.getName(), (statement, i, val) -> statement.setLong(i, toGSTime(((Timestamp) val).toInstant())));
        map.put(Date.class.getName(), (statement, i, val) -> statement.setLong(i, ((Date) val).getTime()));
        map.put(java.sql.Date.class.getName(), (statement, i, val) -> statement.setLong(i, ((java.sql.Date) val).getTime()));
        map.put(java.sql.Time.class.getName(), (statement, i, val) -> statement.setLong(i, ((Time) val).getTime()));
        map.put(LocalDate.class.getName(), (statement, i, val) -> statement.setLong(i, ((LocalDate) val).toEpochDay()));
        map.put(LocalTime.class.getName(), (statement, i, val) -> statement.setLong(i, ((LocalTime) val).toNanoOfDay()));
        map.put(LocalDateTime.class.getName(), (statement, i, val) -> statement.setLong(i, toGSTime(((LocalDateTime) val).atZone(ZoneId.of("UTC")).toInstant())));
        return map;
    }

    public static String getPropertyType(String typeName) {
        if (!sqlTypesMap.containsKey(typeName)) {
            throw new IllegalArgumentException("cannot map non trivial type " + typeName);
        }
        return sqlTypesMap.get(typeName);
    }

    public static Object getPropertyValue(ResultSet resultSet, Class<?> propertyType, int index) throws SQLException {
        if (!sqlExtractorsMap.containsKey(propertyType.getName())) {
            throw new IllegalArgumentException("cannot map non trivial type " + propertyType.getName());
        }
        final ExtractFunction extractFunction = sqlExtractorsMap.get(propertyType.getName());
        return extractFunction.extract(resultSet, index);
    }


    public static void setPropertyValue(PreparedStatement statement, Class<?> propertyType, int index, Object value) throws SQLException {
        if (!sqlInjectorsMap.containsKey(propertyType.getName())) {
            throw new IllegalArgumentException("cannot map non trivial type " + propertyType.getName());
        }
        final InjectFunction injectFunction = sqlInjectorsMap.get(propertyType.getName());
        injectFunction.inject(statement, index, value);
    }


    private static long toGSTime(Instant instant) { // long which consists 2 parts: 1. time in milliseconds 2. time in nanos
        return (instant.getEpochSecond() - OFFSET) * NANOS_PER_SEC + instant.getNano();  //
    }

    private static Instant fromGsTime(long l) {
        long secs = l / NANOS_PER_SEC;
        int nanos = (int) (l % NANOS_PER_SEC);
        return Instant.ofEpochSecond(secs + OFFSET, nanos);
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
                return inclusion ? " <= " : " < ";
            case TemplateMatchCodes.LT:
            case TemplateMatchCodes.LE:
                return inclusion ? " >= " : " > ";
            default:
                throw new IllegalStateException("match code " + originalMatchCode + " no supported");
        }
    }

    public static void appendCustomQueryString(ICustomQuery customQuery, StringBuilder queryBuilder, QueryParameters queryParams) {
        if (customQuery instanceof AbstractCompundCustomQuery) {
            StringJoiner joiner = new StringJoiner(customQuery.getClass().equals(CompoundAndCustomQuery.class) ? " AND " : " OR ");
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery) customQuery).get_subQueries();
            for (ICustomQuery subQuery : subQueries) {
                appendCustomQueryString(subQuery, queryBuilder, queryParams);
            }
            queryBuilder.append(joiner);
        } else if (customQuery instanceof Range) {
            SqliteUtils.appendRangeString((Range) customQuery, queryBuilder, queryParams);
        } else {
            throw new IllegalStateException("SQL query of type" + customQuery.getClass().toString() + " is unsupported");
        }
    }

    private static void appendRangeString(Range range, StringBuilder queryBuilder, QueryParameters queryParams) {
        if(!rangeToStringFunctionMap.containsKey(range.getClass().getName())){
            throw new IllegalStateException("SQL query of type" + range.getClass().getName() + " is unsupported");
        }
        rangeToStringFunctionMap.get(range.getClass().getName()).toString(range, queryBuilder, queryParams);

    }

    static TemplateMatchTier evaluateByMatchTier(ITemplateHolder template, TemplateMatchTier templateMatchTier) {
        if (templateMatchTier == TemplateMatchTier.MATCH_HOT_AND_COLD) {
            return template.isMemoryOnlySearch() ? TemplateMatchTier.MATCH_HOT :
                    (template.getBatchOperationContext() != null && template.getBatchOperationContext().getMaxEntries() < Integer.MAX_VALUE ?
                            templateMatchTier : TemplateMatchTier.MATCH_COLD);
        }
        return templateMatchTier;
    }

    static TemplateMatchTier getTemplateMatchTier(Range criteria, ITemplateHolder template, String timeType) {
        if (template.getCustomQuery() != null) {
            ICustomQuery customQuery = template.getCustomQuery();
            return evalCustomQuery(criteria, customQuery, timeType);
        } else {
            ITypeDesc typeDesc = template.getServerTypeDesc().getTypeDesc();
            int index = ((PropertyInfo) typeDesc.getFixedProperty(criteria.getPath())).getOriginalIndex();
            TemplateEntryData entryData = template.getTemplateEntryData();
            Object value = entryData.getFixedPropertyValue(index);

            if (template.getExtendedMatchCodes() == null || (template.isIdQuery() && typeDesc.getIdPropertyName().equalsIgnoreCase(criteria.getPath()))) {
                if (value == null) {
                    return TemplateMatchTier.MATCH_HOT_AND_COLD;
                } else if (timeType != null) {
                    value = convertTimeTypeToInstant(value);
                }
                return (criteria.getPredicate().execute(value) ? TemplateMatchTier.MATCH_HOT : TemplateMatchTier.MATCH_COLD);
            } else if (template.getExtendedMatchCodes() != null) {
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : evalRange(criteria, getRangeFromMatchCode(entryData, value, index, criteria.getPath()), timeType);
            }
        }
        return TemplateMatchTier.MATCH_HOT_AND_COLD;
    }

    static TemplateMatchTier getTemplateMatchTier(Range criteria, ITemplatePacket packet, String timeType) {
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
                if (value == null) {
                    return TemplateMatchTier.MATCH_HOT_AND_COLD;
                } else if (timeType != null) {
                    value = convertTimeTypeToInstant(value);
                }
                return (criteria.getPredicate().execute(value) ? TemplateMatchTier.MATCH_HOT : TemplateMatchTier.MATCH_COLD);
            } else if (hasMatchCodes(packet)) {
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : SqliteUtils.evalRange(criteria, ((QueryTemplatePacket) packet).getRanges().get(criteria.getPath()), timeType); //todo
            }
        }
        return TemplateMatchTier.MATCH_HOT_AND_COLD;
    }

    static Instant convertTimeTypeToInstant(Object value) {
        if (Instant.class.equals(value.getClass())) {
            return (Instant) value;
        } else if (Timestamp.class.equals(value.getClass())) {
            return ((Timestamp) value).toInstant();
        } else if (Long.class.equals(value.getClass())) {
            return Instant.ofEpochMilli((long) value);
        } else if (Date.class.equals(value.getClass())) {
            return Instant.ofEpochMilli(((Date) value).getTime());
        } else if (LocalDateTime.class.equals(value.getClass())) {
            return ((LocalDateTime) value).toInstant(ZoneOffset.UTC);
        }
        throw new IllegalStateException("Time type of " + value.getClass().toString() + " is unsupported");
    }

    private static Range convertRangeFromTimestampToInstant(Range queryValueRange) {
        if (queryValueRange.isEqualValueRange()) {
            Timestamp value = (Timestamp) ((EqualValueRange) queryValueRange).getValue();
            return new EqualValueRange(queryValueRange.getPath(), value.toInstant());
        } else if (queryValueRange.isSegmentRange()) {
            SegmentRange segmentRange = (SegmentRange) queryValueRange;
            Comparable<Instant> minInstant = segmentRange.getMin() != null ? ((Timestamp) segmentRange.getMin()).toInstant() : null;
            Comparable<Instant> maxInstant = segmentRange.getMax() != null ? ((Timestamp) segmentRange.getMax()).toInstant() : null;
            return new SegmentRange(queryValueRange.getPath(), minInstant, ((SegmentRange) queryValueRange).isIncludeMin(), maxInstant, ((SegmentRange) queryValueRange).isIncludeMax());
        }
        throw new IllegalStateException("Supports only equal and segment Range");
    }

    private static Range convertRangeFromLongToInstant(Range queryValueRange) {
        if (queryValueRange.isEqualValueRange()) {
            long value = (long) ((EqualValueRange) queryValueRange).getValue();
            return new EqualValueRange(queryValueRange.getPath(), Instant.ofEpochMilli(value));
        } else if (queryValueRange.isSegmentRange()) {
            SegmentRange segmentRange = (SegmentRange) queryValueRange;
            Comparable<Instant> minInstant = segmentRange.getMin() != null ? Instant.ofEpochMilli((long) segmentRange.getMin()) : null;
            Comparable<Instant> maxInstant = segmentRange.getMax() != null ? Instant.ofEpochMilli((long) segmentRange.getMax()) : null;
            return new SegmentRange(queryValueRange.getPath(), minInstant, ((SegmentRange) queryValueRange).isIncludeMin(), maxInstant, ((SegmentRange) queryValueRange).isIncludeMax());
        }
        throw new IllegalStateException("Supports only equal and segment Range");
    }

    private static Range convertRangeFromJavaUtilDateToInstant(Range queryValueRange) {
        if (queryValueRange.isEqualValueRange()) {
            Date value = (Date) ((EqualValueRange) queryValueRange).getValue();
            return new EqualValueRange(queryValueRange.getPath(), value.toInstant());
        } else if (queryValueRange.isSegmentRange()) {
            SegmentRange segmentRange = (SegmentRange) queryValueRange;
            Comparable<Instant> minInstant = segmentRange.getMin() != null ? ((Date) segmentRange.getMin()).toInstant() : null;
            Comparable<Instant> maxInstant = segmentRange.getMax() != null ? ((Date) segmentRange.getMax()).toInstant() : null;
            return new SegmentRange(queryValueRange.getPath(), minInstant, ((SegmentRange) queryValueRange).isIncludeMin(), maxInstant, ((SegmentRange) queryValueRange).isIncludeMax());
        }
        throw new IllegalStateException("Supports only equal and segment Range");
    }

    private static Range convertRangeFromLocalDateTimeToInstant(Range queryValueRange) {
        if (queryValueRange.isEqualValueRange()) {
            LocalDateTime value = (LocalDateTime) ((EqualValueRange) queryValueRange).getValue();
            return new EqualValueRange(queryValueRange.getPath(), value.toInstant(ZoneOffset.UTC));
        } else if (queryValueRange.isSegmentRange()) {
            SegmentRange segmentRange = (SegmentRange) queryValueRange;
            Comparable<Instant> minInstant = segmentRange.getMin() != null ? ((LocalDateTime) segmentRange.getMin()).toInstant(ZoneOffset.UTC) : null;
            Comparable<Instant> maxInstant = segmentRange.getMax() != null ? ((LocalDateTime) segmentRange.getMax()).toInstant(ZoneOffset.UTC) : null;
            return new SegmentRange(queryValueRange.getPath(), minInstant, ((SegmentRange) queryValueRange).isIncludeMin(), maxInstant, ((SegmentRange) queryValueRange).isIncludeMax());
        }
        throw new IllegalStateException("Supports only equal and segment Range");
    }

    private static TemplateMatchTier evalRange(Range criteria, Range queryValueRange, String timeType) {
        if (timeType != null) {
            if (queryValueRange.isSegmentRange() || queryValueRange.isEqualValueRange()) {
                if (Timestamp.class.getName().equals(timeType)) {
                    queryValueRange = convertRangeFromTimestampToInstant(queryValueRange);
                } else if (Long.class.getName().equals(timeType) || long.class.getName().equals(timeType)) {
                    queryValueRange = convertRangeFromLongToInstant(queryValueRange);
                } else if (Date.class.getName().equals(timeType)) {
                    queryValueRange = convertRangeFromJavaUtilDateToInstant(queryValueRange);
                } else if (LocalDateTime.class.getName().equals(timeType)) {
                    queryValueRange = convertRangeFromLocalDateTimeToInstant(queryValueRange);
                }
            } else {
                return TemplateMatchTier.MATCH_COLD;
            }
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
                boolean someMatched = false;
                for (Object criteriaInRangeInValue : criteriaInRange.getInValues()) {
                    boolean match = queryValueRange.getPredicate().execute(criteriaInRangeInValue);
                    someMatched |= match;
                }
                return someMatched ? TemplateMatchTier.MATCH_HOT_AND_COLD : TemplateMatchTier.MATCH_COLD;
            }
        }
        return TemplateMatchTier.MATCH_COLD;
    }

    private static TemplateMatchTier evalCustomQuery(Range criteria, ICustomQuery customQuery, String timeType) {
        if (customQuery instanceof CompoundAndCustomQuery) {
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery) customQuery).get_subQueries();
            TemplateMatchTier result = null;
            for (ICustomQuery query : subQueries) {
                TemplateMatchTier templateMatchTier = evalCustomQuery(criteria, query, timeType);
                if (result == null) {
                    result = templateMatchTier;
                } else {
                    result = andTemplateMatchTier(result, templateMatchTier);
                }
            }
            return result;
        } else if (customQuery instanceof CompoundOrCustomQuery) {
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery) customQuery).get_subQueries();
            TemplateMatchTier result = null;
            for (ICustomQuery query : subQueries) {
                TemplateMatchTier templateMatchTier = evalCustomQuery(criteria, query, timeType);
                if (result == null) {
                    result = templateMatchTier;
                } else {
                    result = orTemplateMatchTier(result, templateMatchTier);
                }
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

    static private TemplateMatchTier andTemplateMatchTier(TemplateMatchTier tier1, TemplateMatchTier tier2) {
        if (tier1 == TemplateMatchTier.MATCH_HOT || tier2 == TemplateMatchTier.MATCH_HOT) {
            return TemplateMatchTier.MATCH_HOT;
        } else if (tier1 == TemplateMatchTier.MATCH_COLD || tier2 == TemplateMatchTier.MATCH_COLD) {
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
            boolean someMatched = false;
            for (Object queryInRangeInValue : queryValueRange.getInValues()) {
                boolean match = criteria.getPredicate().execute(queryInRangeInValue);
                allMatched &= match;
                someMatched |= match;
                //noneMatched = noneMatched ? !match : noneMatched;
            }

            return allMatched ? TemplateMatchTier.MATCH_HOT :
                    (someMatched ? TemplateMatchTier.MATCH_HOT_AND_COLD : TemplateMatchTier.MATCH_COLD);
        }
    }

    private static Range getRangeFromMatchCode(TemplateEntryData entryData, Object value, int index, String path) {
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

    @FunctionalInterface
    interface ExtractFunction {
        Object extract(ResultSet resultSet, int index) throws SQLException;
    }

    @FunctionalInterface
    interface InjectFunction {
        void inject(PreparedStatement statement, int index, Object value) throws SQLException;
    }

    @FunctionalInterface
    interface RangeToStringFunction {
        void toString(Range range, StringBuilder queryBuilder, QueryParameters queryParams) ;
    }

}
