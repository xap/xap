package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.jdbc.builder.range.Range;

public class CriteriaRangePredicate implements CachePredicate, InternalCachePredicate {
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
    public TemplateMatchTier evaluate(ITemplateHolder template) {
        TemplateMatchTier templateMatchTier = SqliteUtils.getTemplateMatchTier(criteria, template, null);
        return SqliteUtils.evaluateByMatchTier(template, templateMatchTier);
    }

    //For tests
    @Override
    public TemplateMatchTier evaluate(ITemplatePacket packet) {
        return SqliteUtils.getTemplateMatchTier(criteria, packet, null);
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        return criteria.getPredicate().execute(entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition(criteria.getPath())));
    }

    @Override
    public String toString() {
        return "CriteriaPredicate{" +
                "typeName='" + typeName + '\'' +
                ", criteria='" + criteria + '\'' +
                '}';
    }
}
