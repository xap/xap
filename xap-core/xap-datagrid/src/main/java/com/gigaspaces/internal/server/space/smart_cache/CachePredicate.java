package com.gigaspaces.internal.server.space.smart_cache;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;

public interface CachePredicate {

    boolean evaluate(ITemplatePacket packet); // check if query fits rule - for querying

    boolean evaluate(IEntryData entryData); // check if  entry fits rule - for evicting
}
