package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cache.context.TemplateMatchTier;

@InternalApi
public interface InternalCachePredicate extends CachePredicate {
    TemplateMatchTier evaluate(ITemplatePacket template);
}
