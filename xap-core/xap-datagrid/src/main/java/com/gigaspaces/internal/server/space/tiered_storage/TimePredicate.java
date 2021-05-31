package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.time.Duration;
import java.time.Instant;

public class TimePredicate implements CachePredicate, InternalCachePredicate {
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

    //eviction from hot tier
    public long getExpirationTime(Object value, long gracePeriod) {
        if (value == null){
            throw new SpaceMetadataException("property of type [" + timeColumn + "] which set as time rule cannot be null.");
        }
        long originalValueInLong = Long.class.equals(value.getClass())? (long)value : SqliteUtils.convertTimeTypeToInstant(value).toEpochMilli();
        return originalValueInLong + period.toMillis() + gracePeriod;
    }

    //For tests
    @Override
    public TemplateMatchTier evaluate(ITemplatePacket packet) {
        String columnTimeType = packet.getTypeDescriptor().getFixedProperty(getTimeColumn()).getType().getName();
        return SqliteUtils.getTemplateMatchTier(getTimeRuleAsInstantRange(), packet, columnTimeType);
    }

    @Override
    public boolean evaluate(IEntryData entryData)  {
        Object value = entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition(timeColumn));
        value = SqliteUtils.convertTimeTypeToInstant(value);
        return getTimeRuleAsInstantRange().getPredicate().execute(value);
    }

    @Override
    public TemplateMatchTier evaluate(ITemplateHolder template) {
       String timeType = template.getServerTypeDesc().getTypeDesc().getFixedProperty(timeColumn).getTypeName();
       TemplateMatchTier templateMatchTier = SqliteUtils.getTemplateMatchTier(getTimeRuleAsInstantRange(), template, timeType);
       return SqliteUtils.evaluateByMatchTier(template, templateMatchTier);
    }

    @Override
    public boolean isTimeRule(){
        return true;
    }

    public SegmentRange getTimeRuleAsInstantRange(){
         Instant timeRule = Instant.now().minus(period);
         return new SegmentRange(timeColumn, timeRule, true, null, false);
    }

    public SegmentRange getTimeRuleAsTypedRange(String typeName){
        Instant timeRule = Instant.now().minus(period);
        return new SegmentRange(timeColumn, SqliteUtils.convertInstantToDateType(timeRule, typeName), true, null, false);
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
