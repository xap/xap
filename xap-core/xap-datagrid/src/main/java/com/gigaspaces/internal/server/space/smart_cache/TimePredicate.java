package com.gigaspaces.internal.server.space.smart_cache;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;

import java.time.Duration;

public class TimePredicate implements CachePredicate {
    private final String typeName;
    private final String timeColumn;
    private final Duration period;

    public TimePredicate(String typeName, String timeColumn, Duration period) {
        this.typeName = typeName;
        this.timeColumn = timeColumn;
        this.period = period;
    }

    public String getTypeName() {
        return typeName;
    }

    public Duration getPeriod() {
        return period;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    @Override
    public boolean evaluate(ITemplatePacket packet) {
        return false; //TODO
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        return false; //TODO
    }

    @Override
    public String toString() {
        return "TimePredicate{" +
                "typeName='" + typeName + '\'' +
                ", timeColumn='" + timeColumn + '\'' +
                ", period=" + period +
                '}';
    }
}
