package com.gigaspaces.jdbc.calcite.pg;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PgOidGenerator {
    public static final PgOidGenerator INSTANCE = new PgOidGenerator();
    private final AtomicInteger cntr = new AtomicInteger();
    private final HashMap<String, Integer> items = new HashMap<>();
    public int oid(String fqn) {
        return items.computeIfAbsent(fqn, n -> cntr.incrementAndGet());
    }
}
