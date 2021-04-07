package com.j_spaces.jdbc.tiered;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.AbstractCompundCustomQuery;
import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.InRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public class TieredUtils {

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
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : evalRange(criteria, ((QueryTemplatePacket) packet).getRanges().get(criteria.getPath()), timeType);
            }
        }
        return TemplateMatchTier.MATCH_HOT_AND_COLD;
    }

    static Object convertTimeTypeToInstant(Object value) {
        if (Instant.class.equals(value.getClass())) {
            return value;
        } else if (Timestamp.class.equals(value.getClass())) {
            return ((Timestamp) value).toInstant();
        } else if (Long.class.equals(value.getClass())) {
            return Instant.ofEpochMilli((long) value);
        }
        throw new IllegalStateException("Time type of " + value.getClass().toString() + " is unsupported");
    }

    private static Range convertRangeFromTimestampToInstant(Range queryValueRange) {
        if (queryValueRange.isEqualValueRange()) {
            Timestamp timestamp = (Timestamp) ((EqualValueRange) queryValueRange).getValue();
            return new EqualValueRange(queryValueRange.getPath(), timestamp.toInstant());
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

    private static TemplateMatchTier evalRange(Range criteria, Range queryValueRange, String timeType) {
        if (timeType != null) {
            if (queryValueRange.isSegmentRange() || queryValueRange.isEqualValueRange()) {
                if (Timestamp.class.getName().equals(timeType)) {
                    queryValueRange = convertRangeFromTimestampToInstant(queryValueRange);
                } else if (Long.class.getName().equals(timeType) || long.class.getName().equals(timeType)) {
                    queryValueRange = convertRangeFromLongToInstant(queryValueRange);
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
        if (tier1 == TemplateMatchTier.MATCH_COLD || tier2 == TemplateMatchTier.MATCH_COLD) {
            return TemplateMatchTier.MATCH_COLD;
        }
        if (tier1 == TemplateMatchTier.MATCH_HOT || tier2 == TemplateMatchTier.MATCH_HOT) {
            return TemplateMatchTier.MATCH_HOT;
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

    private static boolean hasMatchCodes(ITemplatePacket packet) {
        Object[] fieldValues = packet.getFieldValues();
        for (Object fieldValue : fieldValues) {
            if (fieldValue != null)
                return true;
        }
        return false;
    }

    public enum TemplateMatchTier {
        MATCH_HOT, MATCH_COLD, MATCH_HOT_AND_COLD;
    }
}