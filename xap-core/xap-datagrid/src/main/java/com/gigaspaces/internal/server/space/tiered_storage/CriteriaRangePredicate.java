package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.AbstractCompundCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.TemplateEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.*;

import java.util.List;
import java.util.Set;

public class CriteriaRangePredicate implements CachePredicate {
    public final boolean isTransient;
    private final String typeName;
    private final Range criteria;

    public CriteriaRangePredicate(String typeName, Range criteria, boolean isTransient) {
        this.typeName = typeName;
        this.criteria = criteria;
        this.isTransient = isTransient;
    }

    public String getTypeName() {
        return typeName;
    }

    public Range getCriteria() {
        return criteria;
    }

    @Override
    public boolean evaluate(ITemplateHolder template) {
        if (template.getCustomQuery() != null) {
            ICustomQuery customQuery = template.getCustomQuery();
            return evalCustomQuery(customQuery);
        } else {
            ITypeDesc typeDesc = template.getServerTypeDesc().getTypeDesc();
            int index = ((PropertyInfo) typeDesc.getFixedProperty(criteria.getPath())).getOriginalIndex();
            TemplateEntryData entryData = template.getTemplateEntryData();
            if (template.getExtendedMatchCodes() == null) {//by template
                Object value = entryData.getFixedPropertyValue(index);
                return value != null && criteria.getPredicate().execute(value);
            } else if (template.isIdQuery() && typeDesc.getIdPropertyName().equalsIgnoreCase(criteria.getPath())) {
                return criteria.getPredicate().execute(entryData.getFixedPropertyValue(index));
            } else if (template.getExtendedMatchCodes() != null) {
                Object fieldValue = entryData.getFixedPropertyValue(index);
                if (fieldValue == null) {
                    return true;
                } else {
                    return evalRange(getRange(entryData.getExtendedMatchCodes()[index], criteria.getPath(), fieldValue));
                }
            }
        }
        return false;
    }

    private Range getRange(int matchCode, String path, Object fieldValue) {
        switch (matchCode) {
            case TemplateMatchCodes.EQ:
                return new EqualValueRange(path, fieldValue);
            case TemplateMatchCodes.NE:
                return new NotEqualValueRange(path, fieldValue);
            case TemplateMatchCodes.LT:
                return new SegmentRange(path, null, true, (Comparable<?>) fieldValue, false);
            case TemplateMatchCodes.LE:
                return new SegmentRange(path, null, true, (Comparable<?>) fieldValue, true);
            case TemplateMatchCodes.GT:
                return new SegmentRange(path, (Comparable<?>) fieldValue, true, null, false);
            case TemplateMatchCodes.GE:
                return new SegmentRange(path, (Comparable<?>) fieldValue, true, null, true);
            default:
                throw new IllegalArgumentException("match codes with code "+matchCode);
        }
    }

    @Override
    public boolean evaluate(ITemplatePacket packet) {
        if (packet.getCustomQuery() != null) {
            ICustomQuery customQuery = packet.getCustomQuery();
            return evalCustomQuery(customQuery);
        } else {
            SpacePropertyDescriptor property = packet.getTypeDescriptor().getFixedProperty(criteria.getPath());
            if (property == null) {
                throw new IllegalStateException("type "+packet.getTypeDescriptor().getTypeName()+" reached CriteriaRangePredicate.evaluate but has no property "+criteria.getPath());
            }
            int index = ((PropertyInfo) property).getOriginalIndex();
            if (packet instanceof TemplatePacket) {
                return criteria.getPredicate().execute(packet.getFieldValue(index));
            } else if (packet.isIdQuery() && packet.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(criteria.getPath())) {
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
            if ((queryValueRange.isEqualValueRange())) {
                EqualValueRange valueRange = (EqualValueRange) queryValueRange;
                return criteria.getPath().equals(valueRange.getPath()) &&
                        criteria.getPredicate().execute(valueRange.getValue());
            } else if (queryValueRange instanceof InRange) {
                Set inValues = ((InRange) queryValueRange).getInValues();
                return inValues.size() == 1 && criteria.getPredicate().execute(inValues.toArray()[0]);
            }
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
            } else if (queryValueRange instanceof InRange) {
                for (Object val : ((InRange) queryValueRange).getInValues()) {
                    if (!criteria.getPredicate().execute(val)) {
                        return false;
                    }
                }
                return true;
            }
        } else if (criteria instanceof InRange) {
            if (queryValueRange.isEqualValueRange()) {
                return criteria.getPredicate().execute(((EqualValueRange) queryValueRange).getValue());
            } else if (queryValueRange instanceof InRange) {
                for (Object val : ((InRange) queryValueRange).getInValues()) {
                    if (!criteria.getPredicate().execute(val)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        return criteria.getPredicate().execute(entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition(criteria.getPath())));
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }


    @Override
    public String toString() {
        return "CriteriaPredicate{" +
                "typeName='" + typeName + '\'' +
                ", criteria='" + criteria + '\'' +
                '}';
    }
}
