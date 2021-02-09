package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;

public interface CachePredicate {

    boolean evaluate(ITemplatePacket packet); // check if query fits rule - for querying

    boolean evaluate(IEntryData entryData); // check if  entry fits rule - for evicting

    boolean isTransient();
}
