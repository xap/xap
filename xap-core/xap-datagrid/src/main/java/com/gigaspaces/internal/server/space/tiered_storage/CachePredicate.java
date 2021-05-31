package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.context.TemplateMatchTier;

public interface CachePredicate {

    boolean evaluate(IEntryData entryData); // check if  entry fits rule - for evicting

    default boolean isTransient(){
        return false;
    }

    TemplateMatchTier evaluate(ITemplateHolder template);

    default boolean isTimeRule(){
        return false;
    }
}