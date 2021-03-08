package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.context.TemplateMatchTier;

public interface CachePredicate {

    boolean evaluate(IEntryData entryData); // check if  entry fits rule - for evicting

    boolean isTransient();

    TemplateMatchTier evaluate(ITemplateHolder template);
}