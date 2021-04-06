package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TimePredicate implements CachePredicate, InternalCachePredicate {
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
    public long getExpirationTime(IEntryPacket entry, long gracePeriod) {
        //assumes time column field not null when this method is reached
        Object value = entry.getPropertyValue(getTimeColumn());
        long originalValueInLong = Long.class.equals(value.getClass())? (long)value : SqliteUtils.convertTimeTypeToInstant(value).toEpochMilli();
        return originalValueInLong + period.toMillis() + gracePeriod;
    }

    //For tests
    @Override
    public TemplateMatchTier evaluate(ITemplatePacket packet) {
        String columnTimeType = packet.getTypeDescriptor().getFixedProperty(getTimeColumn()).getType().getName();
        return SqliteUtils.getTemplateMatchTier(getTimeRuleAsRange(), packet, columnTimeType);
    }

    @Override
    public boolean evaluate(IEntryData entryData)  {
        Object value = entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition(timeColumn));
        value = SqliteUtils.convertTimeTypeToInstant(value);
        return getTimeRuleAsRange().getPredicate().execute(value);
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }

    @Override
    public TemplateMatchTier evaluate(ITemplateHolder template) {
       String timeType = template.getServerTypeDesc().getTypeDesc().getFixedProperty(timeColumn).getTypeName();
       TemplateMatchTier templateMatchTier = SqliteUtils.getTemplateMatchTier(getTimeRuleAsRange(), template, timeType);
       return SqliteUtils.evaluateByMatchTier(template, templateMatchTier);
    }

    @Override
    public boolean isTimeRule(){
        return true;
    }

    public SegmentRange getTimeRuleAsRange(){
         Instant timeRule = Instant.now().minus(period);
         return new SegmentRange(timeColumn, timeRule, true, null, false);
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
