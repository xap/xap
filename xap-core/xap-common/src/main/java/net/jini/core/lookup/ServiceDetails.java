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
package net.jini.core.lookup;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy (shay.banon)
 */
@com.gigaspaces.api.InternalApi
public class ServiceDetails implements SmartExternalizable {
    private static final long serialVersionUID = -7773947979481705736L;

    private ServiceID lusId;

    private boolean exists;

    private long registrationTime;

    public ServiceDetails() {
    }

    public ServiceDetails(ServiceID lusId, boolean exists, long registrationTime) {
        this.lusId = lusId;
        this.exists = exists;
        this.registrationTime = registrationTime;
    }

    public ServiceID getLusId() {
        return lusId;
    }

    public boolean isExists() {
        return exists;
    }

    public long getRegistrationTime() {
        return registrationTime;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(lusId);
        out.writeBoolean(exists);
        out.writeLong(registrationTime);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        lusId = (ServiceID) in.readObject();
        exists = in.readBoolean();
        registrationTime = in.readLong();
    }
}
