package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.AbstractCompundCustomQuery;
import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.TemplateEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.*;

import java.util.List;

public class CriteriaRangePredicate implements CachePredicate, InternalCachePredicate {
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
    public TemplateMatchTier evaluate(ITemplateHolder template) {
        TemplateMatchTier templateMatchTier = SqliteUtils.getTemplateMatchTier(criteria, template, null);
        return SqliteUtils.evaluateByMatchTier(template, templateMatchTier);
    }

    //For tests
    @Override
    public TemplateMatchTier evaluate(ITemplatePacket packet) {
        return SqliteUtils.getTemplateMatchTier(criteria, packet, null);
        //return getTemplateMatchTier(packet);
    }

    /*private TemplateMatchTier getTemplateMatchTier(ITemplateHolder template) {
        if (template.getCustomQuery() != null) {
            ICustomQuery customQuery = template.getCustomQuery();
            return evalCustomQuery(customQuery);
        } else {
            ITypeDesc typeDesc = template.getServerTypeDesc().getTypeDesc();
            int index = ((PropertyInfo) typeDesc.getFixedProperty(criteria.getPath())).getOriginalIndex();
            TemplateEntryData entryData = template.getTemplateEntryData();
            Object value = entryData.getFixedPropertyValue(index);
            if (template.getExtendedMatchCodes() == null || template.isIdQuery() && typeDesc.getIdPropertyName().equalsIgnoreCase(criteria.getPath())) {
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : (criteria.getPredicate().execute(value) ? TemplateMatchTier.MATCH_HOT : TemplateMatchTier.MATCH_COLD);
            } else if (template.getExtendedMatchCodes() != null) {
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : evalRange(getRangeFromMatchCode(entryData, value, index));
            }
        }
        return TemplateMatchTier.MATCH_HOT_AND_COLD;
    }*/

    /*private TemplateMatchTier getTemplateMatchTier(ITemplatePacket packet) {
        if (packet.getCustomQuery() != null) {
            ICustomQuery customQuery = packet.getCustomQuery();
            return evalCustomQuery(customQuery);
        } else {
            SpacePropertyDescriptor property = packet.getTypeDescriptor().getFixedProperty(criteria.getPath());
            if (property == null) {
                throw new IllegalStateException("type " + packet.getTypeDescriptor().getTypeName() + " reached CriteriaRangePredicate.evaluate but has no property " + criteria.getPath());
            }
            int index = ((PropertyInfo) property).getOriginalIndex();
            Object value = packet.getFieldValue(index);
            if (packet instanceof TemplatePacket || (packet.isIdQuery() && packet.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(criteria.getPath()))) {
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : (criteria.getPredicate().execute(value) ? TemplateMatchTier.MATCH_HOT : TemplateMatchTier.MATCH_COLD);
            } else if (hasMatchCodes(packet)) {
                return value == null ? TemplateMatchTier.MATCH_HOT_AND_COLD : evalRange(((QueryTemplatePacket) packet).getRanges().get(criteria.getPath()));
            }
        }
        return TemplateMatchTier.MATCH_HOT_AND_COLD;
    }*/

    /*private Range getRangeFromMatchCode(TemplateEntryData entryData, Object value, int index) {
        short matchCode = entryData.getExtendedMatchCodes()[index];
        String path = criteria.getPath();
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
    }*/

    /*private boolean hasMatchCodes(ITemplatePacket packet) {
        Object[] fieldValues = packet.getFieldValues();
        for (Object fieldValue : fieldValues) {
            if (fieldValue != null)
                return true;
        }
        return false;
    }
    //which types of customQuery can be? or, and, range
    /*private TemplateMatchTier evalCustomQuery(ICustomQuery customQuery) {
        if (customQuery instanceof CompoundAndCustomQuery) {
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery) customQuery).get_subQueries();
            TemplateMatchTier result = null;
            for (ICustomQuery query : subQueries) {
                TemplateMatchTier templateMatchTier = evalCustomQuery(query);
                if (result == null) {
                    result = templateMatchTier;
                }
                else {
                    result = andTemplateMatchTier(result, evalCustomQuery(query));
                }
            }
            return result;
        } else if (customQuery instanceof CompoundOrCustomQuery) {
            List<ICustomQuery> subQueries = ((AbstractCompundCustomQuery) customQuery).get_subQueries();
            TemplateMatchTier result = null;
            for (ICustomQuery query : subQueries){
                TemplateMatchTier templateMatchTier = evalCustomQuery(query);
                if (result == null) {
                    result = templateMatchTier;
                }
                else {
                    result = orTemplateMatchTier(result, templateMatchTier);
                }
            }
            return result;
        } else if (customQuery instanceof Range) {
            Range queryValueRange = (Range) customQuery;
            if (queryValueRange.getPath().equalsIgnoreCase(criteria.getPath())) {
                return evalRange(queryValueRange);
            } else {
                return TemplateMatchTier.MATCH_HOT_AND_COLD;
            }
        }
        return TemplateMatchTier.MATCH_COLD;
    }

    private TemplateMatchTier andTemplateMatchTier(TemplateMatchTier tier1, TemplateMatchTier tier2) {
        if (tier1 == TemplateMatchTier.MATCH_COLD || tier2 == TemplateMatchTier.MATCH_COLD) {
            return TemplateMatchTier.MATCH_COLD;
        }
        if (tier1 == TemplateMatchTier.MATCH_HOT || tier2 == TemplateMatchTier.MATCH_HOT) {
            return TemplateMatchTier.MATCH_HOT;
        }
        return TemplateMatchTier.MATCH_HOT_AND_COLD;
    }

    private TemplateMatchTier orTemplateMatchTier(TemplateMatchTier tier1, TemplateMatchTier tier2) {
        if (tier1 == TemplateMatchTier.MATCH_HOT && tier2 == TemplateMatchTier.MATCH_HOT) {
            return TemplateMatchTier.MATCH_HOT;
        } else if (tier1 == TemplateMatchTier.MATCH_COLD && tier2 == TemplateMatchTier.MATCH_COLD) {
            return TemplateMatchTier.MATCH_COLD;
        } else {
            return TemplateMatchTier.MATCH_HOT_AND_COLD;
        }
    }

    private TemplateMatchTier evalRange(Range queryValueRange) {
        if (queryValueRange.isEqualValueRange()) {
            return getTemplateMatchTierForEqualQuery((EqualValueRange) queryValueRange); //check
        } else if (queryValueRange instanceof InRange) {
            return getTemplateMatchTierForInQuery((InRange) queryValueRange);

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
                //someMatched = false i.e noneMatched
                return someMatched ? TemplateMatchTier.MATCH_HOT_AND_COLD : TemplateMatchTier.MATCH_COLD;
            }
        }
        return TemplateMatchTier.MATCH_COLD;

    }

    private TemplateMatchTier getTemplateMatchTierForEqualQuery(EqualValueRange queryValueRange) {
        if (criteria.getPredicate().execute(queryValueRange.getValue())) {
            return TemplateMatchTier.MATCH_HOT;
        } else {
            return TemplateMatchTier.MATCH_COLD;
        }
    }

    private TemplateMatchTier getTemplateMatchTierForInQuery(InRange queryValueRange) {
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
            boolean allMatched = true;//allMatch true if all true : return hot
            //boolean noneMatched = true;//non match true if all false: return cold
            boolean someMatched  = false;
            for (Object queryInRangeInValue : queryValueRange.getInValues()) {
                boolean match = criteria.getPredicate().execute(queryInRangeInValue);
                allMatched &= match;
                someMatched |= match;
                //noneMatched = noneMatched ? !match : noneMatched;
            }

            return allMatched ? TemplateMatchTier.MATCH_HOT :
                    (someMatched? TemplateMatchTier.MATCH_HOT_AND_COLD : TemplateMatchTier.MATCH_COLD);
                    //(noneMatched ? TemplateMatchTier.MATCH_COLD : TemplateMatchTier.MATCH_HOT_AND_COLD);
        }
    }*/

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
