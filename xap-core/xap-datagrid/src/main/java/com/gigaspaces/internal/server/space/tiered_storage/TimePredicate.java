package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cache.context.TemplateMatchTier;

import java.time.Duration;

public class TimePredicate implements CachePredicate {
    private final String typeName;
    private final String timeColumn;
    private final Duration period;
    private final boolean isTransient;

    public TimePredicate(String typeName, String timeColumn, Duration period, boolean isTransient) {
        this.typeName = typeName;
        this.timeColumn = timeColumn;
        this.period = period;
        this.isTransient = isTransient;
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

    //eviction from hot tier
    public static long getExpirationTime(long currentTime){
        //TODO - tiered storage
        return 0;
    }

    //For tests
    public TemplateMatchTier evaluate(ITemplatePacket packet) {
        return TemplateMatchTier.MATCH_COLD; //TODO - tiered storage
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        return false; //TODO - tiered storage
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }

    @Override
    public TemplateMatchTier evaluate(ITemplateHolder template) {
        return TemplateMatchTier.MATCH_COLD; //TODO - tiered storage
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
