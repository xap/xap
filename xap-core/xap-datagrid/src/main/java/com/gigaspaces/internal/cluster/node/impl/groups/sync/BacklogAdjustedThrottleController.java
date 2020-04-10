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

package com.gigaspaces.internal.cluster.node.impl.groups.sync;

import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.time.SystemTime;

import java.text.NumberFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This will throttle operations according to the backlog size and active state. When a channel is
 * inactive, each thread will be throttled according to maxTPWhenInactive When a channel is active,
 * each thread will be throttled in a manner that will continuously decrease the backlog size.
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class BacklogAdjustedThrottleController
        implements IReplicationThrottleController {
    private final int _defaultOperationPer1milisWhenInactive;
    private final int _minOperationPer1milisWhenActive;
    private final int _threshold;
    private final int _sampleRate;
    private final int _finalStageLength;
    // A logger specific to channel this controller reside in only
    private final Logger _specificLogger;

    private long _lastBacklogTimeStamp = -1;
    private long _lastBacklogSize = -1;
    private float _addaptiveTPPer1milis;
    private long _operations;
    private int _sampleIteration;
    private boolean _atFinalStage;
    private int _calibratedOperationsPer1milis;
    private final boolean _throttleWhenInactive;

    public BacklogAdjustedThrottleController(int maxTPWhenInactive,
                                             int minTPWhenActive, int threshold, int sampleRate,
                                             int finalStageLength, String groupName, String sourceMemberName,
                                             String targetMemberName, boolean throttleWhenInactive) {
        _throttleWhenInactive = throttleWhenInactive;
        _defaultOperationPer1milisWhenInactive = maxTPWhenInactive / 1000;
        _minOperationPer1milisWhenActive = minTPWhenActive / 1000;
        _threshold = threshold;
        _sampleRate = sampleRate;
        _finalStageLength = finalStageLength;
        _addaptiveTPPer1milis = _defaultOperationPer1milisWhenInactive;
        _calibratedOperationsPer1milis = _defaultOperationPer1milisWhenInactive;

        _specificLogger = ReplicationLogUtils.createChannelSpecificLogger(sourceMemberName, targetMemberName, groupName);
    }

    public synchronized boolean throttle(long backlogSize, int contextSize,
                                         boolean channelActive) {
        if (!channelActive && !_throttleWhenInactive)
            return true;

        // Measure thread throughput
        _operations += contextSize;

        float throttleRatePer1ms;

        if (channelActive) {
            // Stop throttling
            if (backlogSize == 0 || backlogSize < _threshold) {
                if (_specificLogger.isDebugEnabled())
                    _specificLogger.debug("backlog size [" + backlogSize
                            + "] is below the threshold [" + _threshold
                            + "], throttling ended");
                return false;
            }

            // We need to throttle in a way that will constantly reduce the
            // backlog size
            // First time
            if (_lastBacklogTimeStamp == -1) {
                _lastBacklogTimeStamp = SystemTime.timeMillis();
                _lastBacklogSize = backlogSize;
                // Keep throttling
                return true;
            } else {
                final long currentTimeStamp = SystemTime.timeMillis();
                final long interval = currentTimeStamp - _lastBacklogTimeStamp;
                // Protect from division by 0
                if (interval >= _sampleRate) {
                    if (!_atFinalStage
                            && backlogSize < (_minOperationPer1milisWhenActive * 1000L * _finalStageLength)) {
                        if (_specificLogger.isDebugEnabled())
                            _specificLogger.debug("Entering final stage, no throttling up allowed");
                        _atFinalStage = true;
                    }

                    // calculate throttling rate
                    final long backlogSizeDifference = backlogSize
                            - _lastBacklogSize;

                    _lastBacklogTimeStamp = currentTimeStamp;
                    _lastBacklogSize = backlogSize;

                    float backlogGrowthRatePerSecond = ((float) backlogSizeDifference / interval) * 1000;

                    final float backlogProprotionalGrowthPerSecond = backlogGrowthRatePerSecond
                            / _lastBacklogSize;
                    // The backlog is increasing its size by a ratio of less
                    // then 1 percent per second, we want to increase the
                    // throttling
                    final float minReductionRateRequired = -0.01F;

                    // Emergency mode, backlog keeps increasing eventhough we
                    // reached minimal throttling
                    if (backlogProprotionalGrowthPerSecond > 0
                            && _addaptiveTPPer1milis <= _minOperationPer1milisWhenActive) {
                        if (_specificLogger.isDebugEnabled())
                            _specificLogger.debug("Throttling does not keep up with backlog increase rate, throttling down to 1000 operations per second");
                        throttleRatePer1ms = 1;
                    }

                    if (backlogProprotionalGrowthPerSecond > minReductionRateRequired) {
                        // Only reduce if we have not reached minimum TP yet
                        if (_addaptiveTPPer1milis > _minOperationPer1milisWhenActive) {
                            // Reduce by percentage proportional to the growth
                            // rate
                            float reduceFactor = 0.98F + ((minReductionRateRequired - backlogProprotionalGrowthPerSecond) / 2);
                            _addaptiveTPPer1milis = Math.max(_minOperationPer1milisWhenActive,
                                    (_addaptiveTPPer1milis * reduceFactor));

                            if (_specificLogger.isTraceEnabled())
                                _specificLogger.trace("Throttling down to "
                                        + NumberFormat.getPercentInstance()
                                        .format(reduceFactor));
                        }
                    } else if (!_atFinalStage
                            && backlogProprotionalGrowthPerSecond <= -0.1) {
                        if (_addaptiveTPPer1milis < _calibratedOperationsPer1milis) {
                            // Increase by 2 percent
                            float throttleUpFactor = 1.02F;
                            _addaptiveTPPer1milis = Math.min(_calibratedOperationsPer1milis,
                                    (_addaptiveTPPer1milis * throttleUpFactor));

                            if (_specificLogger.isTraceEnabled())
                                _specificLogger.trace("Throttling up to "
                                        + NumberFormat.getPercentInstance()
                                        .format(throttleUpFactor));
                        }
                    }
                }
            }

            throttleRatePer1ms = _addaptiveTPPer1milis;
        } else {
            // When inactive, each thread TP is blocked according to
            // configuration
            throttleRatePer1ms = _calibratedOperationsPer1milis;
        }

        if (_operations > throttleRatePer1ms) {
            _sampleIteration += _operations;
            // Display estimated every 3 seconds if TP is kept at the max
            if (_sampleIteration > (_minOperationPer1milisWhenActive * 1000 * 5)
                    && _specificLogger.isDebugEnabled()) {
                _sampleIteration = 0;
                _specificLogger.debug("Throttling, [throttle rate = "
                        + (throttleRatePer1ms * 1000)
                        + " ops/per second, backlog size = " + backlogSize
                        + ", channel active = " + channelActive + "]");
            }

            // Sleep rate should be the context size dividing by throttle rate
            // per 1 ms.
            // i.e if this is a single operation we should sleep 1 ms, if this
            // is a batch operation of
            // size 100 and the throttle rate per 1ms is 10, we should sleep 10
            // ms
            long sleepRate = (long) Math.max(1,
                    Math.ceil(contextSize
                            / throttleRatePer1ms));
            _operations -= (sleepRate * throttleRatePer1ms);
            try {
                // Throttle this thread
                Thread.sleep(sleepRate);
            } catch (InterruptedException e) {
                // Break throttle
                Thread.currentThread().interrupt();
                return false;
            }
        }

        // Keep throttling
        return true;

    }

    public void suggestThroughPut(int throughPut) {
        // Don't treat 0 throughput
        if (throughPut == 0)
            _calibratedOperationsPer1milis = _defaultOperationPer1milisWhenInactive;
        else {
            int throughPutPer1Milis = throughPut / 1000;
            if (throughPutPer1Milis > _defaultOperationPer1milisWhenInactive)
                _calibratedOperationsPer1milis = Math.max(throughPutPer1Milis,
                        _defaultOperationPer1milisWhenInactive);
        }
        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug("calibrated throttle throughput to "
                    + (_calibratedOperationsPer1milis * 1000));
    }

}
