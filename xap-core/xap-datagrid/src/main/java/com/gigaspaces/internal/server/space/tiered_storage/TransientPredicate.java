package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.context.TemplateMatchTier;

public class TransientPredicate implements CachePredicate {

    @Override
    public boolean evaluate(IEntryData entryData) {
        return true;
    }

    @Override
    public boolean isTransient() {
        return true;
    }

    @Override
    public TemplateMatchTier evaluate(ITemplateHolder template) {
        return null;
    }
}
