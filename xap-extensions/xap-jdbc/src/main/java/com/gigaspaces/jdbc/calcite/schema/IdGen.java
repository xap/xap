package com.gigaspaces.jdbc.calcite.schema;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IdGen {
    public static final IdGen INSTANCE = new IdGen();
    private final AtomicInteger cntr = new AtomicInteger();
    private final HashMap<String, Integer> items = new HashMap<>();
    public int oid(String fqn) {
        return items.computeIfAbsent(fqn, n -> cntr.incrementAndGet());
    }
}
