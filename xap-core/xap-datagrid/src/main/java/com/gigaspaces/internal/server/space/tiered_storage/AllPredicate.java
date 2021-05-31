package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.context.TemplateMatchTier;

public class AllPredicate implements CachePredicate {

    public static final String ALL_KEY_WORD = "all";
    private final String typeName;

    public AllPredicate(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        return true;
    }


    @Override
    public TemplateMatchTier evaluate(ITemplateHolder template) {
        return TemplateMatchTier.MATCH_HOT;
    }
}
