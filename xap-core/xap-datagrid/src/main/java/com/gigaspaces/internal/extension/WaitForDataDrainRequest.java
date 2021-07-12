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
package com.gigaspaces.internal.extension;

import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class WaitForDataDrainRequest implements SpaceRequestInfo {

    private long timeout;
    private long minTimeToWait;
    private boolean isDemote;
    private SpaceContext context;

    public WaitForDataDrainRequest() {
    }

    public WaitForDataDrainRequest(long timeout, long minTimeToWait, boolean isDemote) {
        this.timeout = timeout;
        this.minTimeToWait = minTimeToWait;
        this.isDemote = isDemote;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isDemote() {
        return isDemote;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return context;
    }

    @Override
    public void setSpaceContext(SpaceContext spaceContext) {
        this.context = spaceContext;
    }

    public long getMinTimeToWait() {
        return minTimeToWait;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(timeout);
        out.writeLong(minTimeToWait);
        out.writeBoolean(isDemote);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.timeout = in.readLong();
        this.minTimeToWait = in.readLong();
        this.isDemote = in.readBoolean();

    }
}
