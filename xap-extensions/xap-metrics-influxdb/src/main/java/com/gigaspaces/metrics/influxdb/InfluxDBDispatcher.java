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

package com.gigaspaces.metrics.influxdb;

import com.gigaspaces.logger.ActivityLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import static com.gigaspaces.internal.utils.StringUtils.NEW_LINE;

/**
 * @author Niv Ingberg
 * @since 10.2.1
 */
public abstract class InfluxDBDispatcher implements Closeable {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final ActivityLogger activityLogger = new ActivityLogger.Builder("Report dispatch", logger)
            .concurrent()
            .reduceConsecutiveFailuresLogging(10, 100)
            .build();

    public void send(String data) {
        logger.debug("Sending the following data: {}{}", NEW_LINE, data);
        try {
            doSend(data);
            activityLogger.success();
        } catch (IOException e) {
            activityLogger.fail(e, data);
        }
    }

    protected abstract void doSend(String content) throws IOException;

    @Override
    public void close() {
    }
}
