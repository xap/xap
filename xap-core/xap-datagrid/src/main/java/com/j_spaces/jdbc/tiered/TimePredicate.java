package com.j_spaces.jdbc.tiered;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.time.Duration;
import java.time.Instant;

import static com.j_spaces.jdbc.tiered.TieredUtils.convertTimeTypeToInstant;
import static com.j_spaces.jdbc.tiered.TieredUtils.getTemplateMatchTier;

public class TimePredicate {

    private final String typeName;
    private final String timeColumn;
    private final Duration period;
    private final boolean isTransient;

    TimePredicate(String typeName, String timeColumn, Duration period, boolean isTransient) {
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

    public boolean evaluate(ITemplatePacket packet) {
        String columnTimeType = packet.getTypeDescriptor().getFixedProperty(getTimeColumn()).getType().getName();
        TieredUtils.TemplateMatchTier tier = getTemplateMatchTier(getTimeRuleAsRange(), packet, columnTimeType);
        return tier.equals(TieredUtils.TemplateMatchTier.MATCH_HOT);
    }

    public boolean evaluate(IEntryData entryData) {
        Object value = entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition(timeColumn));
        value = convertTimeTypeToInstant(value);
        return getTimeRuleAsRange().getPredicate().execute(value);
    }

    public boolean isTransient() {
        return isTransient;
    }

    private SegmentRange getTimeRuleAsRange() {
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
