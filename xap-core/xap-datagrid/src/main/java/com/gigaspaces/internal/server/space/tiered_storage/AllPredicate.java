package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;

public class AllPredicate implements CachePredicate {

    public static final String ALL_KEY_WORD = "all";
    public boolean isTransient;

    public AllPredicate(boolean isTransient) {
        this.isTransient = isTransient;
    }

    @Override
    public boolean evaluate(ITemplatePacket packet) {
        return true;
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        return true;
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }
}
