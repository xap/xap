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

package com.gigaspaces.metrics;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricSamplerConfig {

    private static final Logger logger = LoggerFactory.getLogger(MetricSamplerConfig.class.getName());

    static final long DEFAULT_SAMPLING_RATE = TimeUnit.SECONDS.toMillis(5);

    private final String name;
    private final long sampleRate;
    private final long reportRate;
    private final int batchSize;

    public MetricSamplerConfig(String name, Long sampleRate, Long reportRate) {
        this.name = name.toLowerCase();
        if (sampleRate == null)
            throw new IllegalArgumentException("sampleRate cannot be null");
        this.sampleRate = sampleRate.longValue();
        this.reportRate = initReportRate(reportRate);
        this.batchSize = this.reportRate == this.sampleRate ? 1 : (int) (this.reportRate / this.sampleRate);
    }

    private long initReportRate(Long reportRate) {
        long reportRateValue = reportRate != null ? reportRate.longValue() : sampleRate;
        if (reportRateValue < sampleRate) {
            logger.warn("reportRate (" + reportRate + ") cannot be less than sampleRate (" + sampleRate +
                    ") - setting reportRate to sampleRate");
            return sampleRate;
        }
        if (reportRateValue != sampleRate && reportRateValue % sampleRate != 0) {
            int batchSize = (int) Math.ceil(reportRateValue / (double) sampleRate);
            reportRateValue = batchSize * sampleRate;
            logger.warn("reportRate was increased from " + reportRate + " to " + reportRateValue +
                    " - must be a multiple of sampleRate (" + sampleRate + ")");
        }
        return reportRateValue;
    }

    public String getName() {
        return name;
    }

    public long getSamplingRate() {
        return sampleRate;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
