package com.gigaspaces.internal.server.space.smart_cache;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.AbstractCompundCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.util.List;

public class CriteriaRangePredicate implements CachePredicate {
    private final String typeName;
    private final Range criteria;

    public CriteriaRangePredicate(String typeName, Range criteria) {
        this.typeName = typeName;
        this.criteria = criteria;
    }

    public String getTypeName() {
        return typeName;
    }

    public Range getCriteria() {
        return criteria;
    }

    @Override
    public boolean evaluate(ITemplatePacket packet) {
        int index = ((PropertyInfo) packet.getTypeDescriptor().getFixedProperty(criteria.getPath())).getOriginalIndex();
        if (packet.getCustomQuery() != null) {
            ICustomQuery customQuery = packet.getCustomQuery();
            return evalCustomQuery(customQuery);
        } else if (packet instanceof TemplatePacket) {
            TemplatePacket templatePacket = (TemplatePacket) packet;
            return criteria.getPredicate().execute(packet.getFieldValue(index));
        } else if (hasMatchCodes(packet)) {
            Object fieldValue = packet.getFieldValue(index);
            if (fieldValue == null) {
                return true;
            } else {
                Range range = ((QueryTemplatePacket) packet).getRanges().get(criteria.getPath());
                return evalRange(range);
            }
        }
        return false;
    }

    private boolean hasMatchCodes(ITemplatePacket packet) {
        Object[] fieldValues = packet.getFieldValues();
        for (Object fieldValue : fieldValues) {
            if (fieldValue != null)
                return true;
        }
        return false;
    }

    public boolean evalCustomQuery(ICustomQuery customQuery) {
        if (customQuery instanceof AbstractCompundCustomQuery) {
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery) customQuery).get_subQueries();
            for (ICustomQuery query : subQueries) {
                if (!evalCustomQuery(query)) {
                    return false;
                }
            }
            return true;
        } else if (customQuery instanceof Range) {
            Range queryValueRange = (Range) customQuery;
            if (queryValueRange.getPath().equalsIgnoreCase(criteria.getPath())) {
                return evalRange(queryValueRange);
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean evalRange(Range queryValueRange) {
        if (criteria.isEqualValueRange()) {
            if (!(queryValueRange.isEqualValueRange())) {
                return false;
            }
            EqualValueRange valueRange = (EqualValueRange) queryValueRange;
            return criteria.getPath().equals(valueRange.getPath()) &&
                    ((EqualValueRange) criteria).getValue().equals(valueRange.getValue());
        } else if (criteria.isSegmentRange()) {
            if (queryValueRange.isEqualValueRange()) {
                return criteria.getPredicate().execute(((EqualValueRange) queryValueRange).getValue());
            } else if (queryValueRange.isSegmentRange()) {
                SegmentRange querySegmentRange = (SegmentRange) queryValueRange;
                SegmentRange criteriaSegmentRange = (SegmentRange) this.criteria;
                if (querySegmentRange.getMin() != null && querySegmentRange.getMax() != null) {
                    return criteria.getPredicate().execute(querySegmentRange.getMin()) && criteria.getPredicate().execute(querySegmentRange.getMax());
                } else if (criteriaSegmentRange.getMin() != null && criteriaSegmentRange.getMax() == null
                        && querySegmentRange.getMin() != null && querySegmentRange.getMax() == null) {
                    return criteria.getPredicate().execute(querySegmentRange.getMin());
                } else if (criteriaSegmentRange.getMin() == null && criteriaSegmentRange.getMax() != null
                        && querySegmentRange.getMin() == null && querySegmentRange.getMax() != null) {
                    return criteria.getPredicate().execute(querySegmentRange.getMax());
                }
            }
        }
        return false;
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        return false; //TODO
    }

    @Override
    public String toString() {
        return "CriteriaPredicate{" +
                "typeName='" + typeName + '\'' +
                ", criteria='" + criteria + '\'' +
                '}';
    }
}
