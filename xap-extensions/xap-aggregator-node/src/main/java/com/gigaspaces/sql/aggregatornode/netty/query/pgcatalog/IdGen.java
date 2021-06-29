package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IdGen {
    private final AtomicInteger cntr = new AtomicInteger();
    private final HashMap<String, Integer> items = new HashMap<>();
    public int oid(String fqn) {
        return items.computeIfAbsent(fqn, n -> cntr.incrementAndGet());
    }
}
