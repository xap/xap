package com.gigaspaces.internal.server.storage;

import com.j_spaces.core.cache.context.TieredState;

import java.io.Serializable;

public class EntryTieredMetaData implements Serializable {
    static final long serialVersionUID = -2094966444743540360L;
    private TieredState tieredState;
    private boolean isIdenticalToCache;

    public EntryTieredMetaData() {
    }

    public TieredState getTieredState() {
        return tieredState;
    }

    public void setTieredState(TieredState tieredState) {
        this.tieredState = tieredState;
    }

    public boolean isIdenticalToCache() {
        return isIdenticalToCache;
    }

    public void setIdenticalToCache(boolean identicalToCache) {
        isIdenticalToCache = identicalToCache;
    }

    public boolean isExist(){
        return tieredState != null;
    }
}
