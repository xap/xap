/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
