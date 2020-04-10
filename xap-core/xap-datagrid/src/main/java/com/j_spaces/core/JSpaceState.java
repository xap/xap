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

package com.j_spaces.core;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Hashtable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class JSpaceState implements ISpaceState, Serializable {
    static final long serialVersionUID = -7933691513130779751L;

    /**
     * The current state
     */
    private volatile int state;

    /**
     * Create a service state object
     */
    public JSpaceState() {
        state = STOPPED;
    }

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ENGINE);

    private final static Map<Object, String> fieldsMap = new Hashtable<Object, String>();

    static {
        try {
            Field[] fields = JSpaceState.class.getFields();
            for (int i = 0; i < fields.length; i++)
                fieldsMap.put(fields[i].get(null), fields[i].getName().toLowerCase());
        } catch (Exception ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.toString(), ex);
            }
        }
    }

    public static String convertToString(Integer state) {
        return fieldsMap.get(state);
    }

    /**
     * Get the current state of the service
     *
     * @return int - The state of the service
     */
    public int getState() {
        return state;
    }

    /**
     * Set the state of the service
     *
     * @param newState - The new state
     */
    public void setState(int newState) {
        if (newState == getState())
            return;


        state = verifyTransition(newState);

    }


    /**
     * Verify the state transition, checking if the proposed new state is allowed from the current
     * state. If the proposed state is not allowed an IllegalStateException will be thrown
     *
     * @param newState - The new state to check
     * @throws IllegalStateException - If the proposed newState is not allowed
     */
    public int verifyTransition(int newState) {
        int currentState = getState();

        boolean validTransition = false;

        switch (newState) {
            case STARTING:
                if (currentState == STOPPED)
                    validTransition = true;
                break;

            case STARTED:
                if (currentState == STARTING)
                    validTransition = true;
                break;

            case STOPPED:
                if (currentState == STARTED || currentState == STARTING)
                    validTransition = true;
                break;

            case ABORTED:
                validTransition = true;
                break;
            case ABORTING:
                validTransition = true;
                break;

            default:
                throw new IllegalStateException("Illegal state [" + newState
                        + "]");

        }

        if (!validTransition)
            throw new IllegalStateException("New state [" + newState
                    + "] not allowed from [" + getState() + "]");

        return newState;
    }

    /**
     * Check if we're terminated completely, abort process is considered complete
     *
     * @return boolean - If the current state is ABORTED, return <code>true</code>
     */
    public boolean isTerminated() {
        return state == ABORTED;
    }

    /**
     * Check if we're aborting or aborted
     *
     * @return boolean - If the current state is ABORTED or ABORTING, return <code>true</code>
     */
    public boolean isAborted() {
        return state == ABORTED || state == ABORTING;
    }

    public boolean isStopped() {
        return state == STOPPED;
    }

    public boolean isStarted() {
        return state == STARTED;
    }
}
