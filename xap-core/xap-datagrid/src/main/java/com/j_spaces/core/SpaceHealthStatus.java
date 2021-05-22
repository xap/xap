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

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.UnmarshalException;

/**
 * Represents a space health status
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceHealthStatus implements SmartExternalizable {
    /** */
    private static final long serialVersionUID = 1L;

    private static final byte SERIAL_VERSION = Byte.MIN_VALUE;

    private Throwable[] healthIssueErrors;

    /**
     * For Externalizable purposes
     */
    public SpaceHealthStatus() {

    }

    /**
     * Constructs a new space health status object
     *
     * @param healthIssueErrors errors representing why the space is unhealthy
     */
    public SpaceHealthStatus(Throwable[] healthIssueErrors) {
        this.healthIssueErrors = healthIssueErrors;

    }

    /**
     * Gets whether the space is unhealthy
     *
     * @return return true  healthIssueErrors is NULL or empty
     */
    public boolean isHealthy() {
        return (healthIssueErrors == null || healthIssueErrors.length == 0);
    }

    /**
     * Returns an exception representing why the space is unhealthy or null if it is healthy
     *
     * @return exception representing why the space is unhealthy or null if it is healthy
     */
    public SpaceUnhealthyException getUnhealthyReason() {
        if (isHealthy())
            return null;
        else
            return new SpaceUnhealthyException(healthIssueErrors);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        if (in.readByte() != SERIAL_VERSION)
            throw new UnmarshalException("Requested version does not match local version. Please make sure you are using the same version on both ends.");
        healthIssueErrors = (Throwable[]) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(SERIAL_VERSION);
        out.writeObject(healthIssueErrors);
    }
}
