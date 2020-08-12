package com.gigaspaces.internal.server.space.repartitioning;

import java.util.List;

public class DeleteBatch extends Batch {

    private final String type;
    private final List<Object> ids;

    DeleteBatch(String type, List<Object> ids) {
        this.type = type;
        this.ids = ids;
    }

    public String getType() {
        return type;
    }

    public List<Object> getIds() {
        return ids;
    }
}
