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

package com.j_spaces.core.filters;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.metrics.MetricConstants;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.filters.entry.ISpaceFilterEntry;
import com.j_spaces.kernel.ScheduledRunner;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This space filter should be provide a space statistics information.
 *
 * @author Michael Konnikov
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class JSpaceStatistics implements ISpaceFilter {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_FILTERS);

    private final StatisticsContext[] _statRepository;
    private long _period;
    private ScheduledRunner _scheduledStats;

    public JSpaceStatistics() {
        _statRepository = new StatisticsContext[FilterOperationCodes.getMaxValue()];
        for (int i = 0; i < _statRepository.length; i++)
            _statRepository[i] = new StatisticsContext();
    }

    public void init(IJSpace space, String filterId, String url, int priority) {
        SpaceImpl spaceImpl = space.getDirectProxy().getSpaceImplIfEmbedded();
        if (_logger.isDebugEnabled())
            _logger.debug("Initializing Space statistics Filter of " + spaceImpl.getNodeName() + " [priority=" + priority + "]");

        _period = Constants.Statistics.DEFAULT_PERIOD;
        Runnable averageCalculator = new Runnable() {
            @Override
            public void run() {
                for (StatisticsContext operationStatistics : _statRepository)
                    operationStatistics.calculateAverage(_period);
            }
        };
        _scheduledStats = new ScheduledRunner(averageCalculator, "Statistics-Task", 0 /*delay*/, _period);

        registerMetricCounters(spaceImpl.getEngine().getMetricRegistrator().extend(MetricConstants.OPERATIONS_METRIC_NAME));
    }

    public void close() {
        if (_scheduledStats != null) {
            _scheduledStats.cancel();
            _scheduledStats = null; //help gc
        }
    }

    private void registerMetricCounters(MetricRegistrator registrator) {
        _statRepository[FilterOperationCodes.BEFORE_EXECUTE].register(registrator, "execute");
        _statRepository[FilterOperationCodes.BEFORE_WRITE].register(registrator, "write");
        _statRepository[FilterOperationCodes.BEFORE_UPDATE].register(registrator, "update");
        _statRepository[FilterOperationCodes.AFTER_CHANGE].register(registrator, "change");
        _statRepository[FilterOperationCodes.BEFORE_READ].register(registrator, "read");
        _statRepository[FilterOperationCodes.AFTER_READ_MULTIPLE].register(registrator, "read-multiple");
        _statRepository[FilterOperationCodes.BEFORE_TAKE].register(registrator, "take");
        _statRepository[FilterOperationCodes.AFTER_TAKE_MULTIPLE].register(registrator, "take-multiple");
        _statRepository[FilterOperationCodes.BEFORE_REMOVE].register(registrator, "lease-expired");
        _statRepository[FilterOperationCodes.BEFORE_NOTIFY].register(registrator, "register-listener");
        _statRepository[FilterOperationCodes.BEFORE_NOTIFY_TRIGGER].register(registrator, "before-listener-trigger");
        _statRepository[FilterOperationCodes.AFTER_NOTIFY_TRIGGER].register(registrator, "after-listener-trigger");
        _statRepository[FilterOperationCodes.TIERED_STORAGE_READ].register(registrator, "tiered-storage-read");
        _statRepository[FilterOperationCodes.TIERED_STORAGE_WRITE].register(registrator, "tiered-storage-write");
    }

    public void process(SpaceContext context, ISpaceFilterEntry subject, int operationCode) {
        _statRepository[operationCode].increment();
    }

    public void process(SpaceContext context, ISpaceFilterEntry[] entries, int operationCode) {
        _statRepository[operationCode].increment();
    }

    public StatisticsContext getStatistics(int operationCode) {
        return _statRepository[operationCode];
    }

    public Map<Integer, StatisticsContext> getStatistics() {
        Map<Integer, StatisticsContext> result = new HashMap<Integer, StatisticsContext>();
        for (int i = 0; i < _statRepository.length; i++) {
            if (_statRepository[i].getCurrentCount() != 0)
                result.put(i, _statRepository[i]);
        }
        return result;
    }

    public String[] toStringArray() {
        Map<Integer, StatisticsContext> stats = getStatistics();
        String[] result = new String[stats.size()];
        int index = 0;
        for (Map.Entry<Integer, StatisticsContext> entry : stats.entrySet())
            result[index++] = entry.getValue().report(entry.getKey(), _period);
        return result;
    }

    public long getPeriod() {
        return _period;
    }

    public void setPeriod(long period) {
        _period = period;
        _scheduledStats.reschedule(_period);
    }
}
