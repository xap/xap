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

package com.gigaspaces.processor;

import com.gigaspaces.common.Data;

import org.openspaces.events.adapter.SpaceDataEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The processor simulates work done no un-processed Data object. The processData accepts a Data
 * object, simulate work by sleeping, and then sets the processed flag to true and returns the
 * processed Data.
 */
public class Processor {

    Logger log = LoggerFactory.getLogger(this.getClass().getName());

    private long workDuration = 100;

    /**
     * Sets the simulated work duration (in milliseconds). Default to 100.
     */
    public void setWorkDuration(long workDuration) {
        this.workDuration = workDuration;
    }

    /**
     * Process the given Data object and returning the processed Data.
     *
     * Can be invoked using OpenSpaces Events when a matching event occurs.
     */
    @SpaceDataEvent
    public Data processData(Data data) {
        // sleep to simulate some work
        try {
            Thread.sleep(workDuration);
        } catch (InterruptedException e) {
            // do nothing
        }
        data.setProcessed(true);
        data.setData("PROCESSED : " + data.getRawData());
        log.info(" ------ PROCESSED : " + data);

        return data;
    }

}
