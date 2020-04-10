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

package com.gigaspaces.feeder;

import com.gigaspaces.common.Data;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.SpaceInterruptedException;
import org.openspaces.core.context.GigaSpaceContext;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A feeder bean starts a scheduled task that writes a new Data objects to the space (in an
 * unprocessed state).
 *
 * <p>The space is injected into this bean using OpenSpaces support for @GigaSpaceContext
 * annotation.
 *
 * <p>The scheduling uses the java.util.concurrent Scheduled Executor Service. It is started and
 * stopped based on Spring life cycle events.
 *
 * @author kimchy
 */
public class Feeder implements InitializingBean, DisposableBean {

    Logger log = LoggerFactory.getLogger(this.getClass().getName());

    private ScheduledExecutorService executorService;

    private ScheduledFuture<?> sf;

    private long numberOfTypes = 10;

    private long defaultDelay = 1000;

    private FeederTask feederTask;

    @GigaSpaceContext
    private GigaSpace gigaSpace;

    /**
     * Sets the number of types that will be used to set {@link com.gigaspaces.common.Data#setType}
     *
     * <p>The type is used as the routing index for partitioned space. This will affect the
     * distribution of Data objects over a partitioned space.
     */
    public void setNumberOfTypes(long numberOfTypes) {
        this.numberOfTypes = numberOfTypes;
    }

    public void setDefaultDelay(long defaultDelay) {
        this.defaultDelay = defaultDelay;
    }

    public void afterPropertiesSet() throws Exception {
        log.info("--- STARTING FEEDER WITH CYCLE [" + defaultDelay + "]");
        executorService = Executors.newScheduledThreadPool(1);
        feederTask = new FeederTask();
        sf = executorService.scheduleAtFixedRate(feederTask, defaultDelay, defaultDelay,
                TimeUnit.MILLISECONDS);
    }

    public void destroy() throws Exception {
        sf.cancel(false);
        sf = null;
        executorService.shutdown();
    }

    public long getFeedCount() {
        return feederTask.getCounter();
    }


    public class FeederTask implements Runnable {

        private long counter = 1;

        public void run() {
            try {
                long time = System.currentTimeMillis();
                Data data = new Data((counter++ % numberOfTypes), "FEEDER " + Long.toString(time));
                gigaSpace.write(data);
                log.info("--- FEEDER WROTE " + data);
            } catch (SpaceInterruptedException e) {
                // ignore, we are being shutdown
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public long getCounter() {
            return counter;
        }
    }


}
