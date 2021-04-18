package com.gigaspaces.jdbc.model.table;

import java.util.concurrent.atomic.AtomicInteger;

public class TempTableNameGenerator {
    private AtomicInteger count = new AtomicInteger(0);

    public String generate() {
        return "TEMP_" + count.incrementAndGet();
    }

}
