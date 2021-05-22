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


package org.openspaces.remoting;

import com.gigaspaces.serialization.SmartExternalizable;
import org.openspaces.pu.service.PlainServiceMonitors;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

/**
 * A generic event container service monitors.
 *
 * @author kimchy
 */
public class RemotingServiceMonitors extends PlainServiceMonitors {

    private static final long serialVersionUID = -7084565982937002812L;

    public static class Attributes {
        public static final String PROCESSED = "processed";
        public static final String FAILED = "failed";
    }

    public static class RemoteServiceStats implements SmartExternalizable {

        private static final long serialVersionUID = 2541853099219414723L;

        private String beanId;
        private long processed;
        private long failed;

        public RemoteServiceStats() {
        }

        public RemoteServiceStats(String beanId, long processed, long failed) {
            this.beanId = beanId;
            this.processed = processed;
            this.failed = failed;
        }

        public String getBeanId() {
            return beanId;
        }

        public long getProcessed() {
            return processed;
        }

        public long getFailed() {
            return failed;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(beanId);
            out.writeLong(processed);
            out.writeLong(failed);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            beanId = (String) in.readObject();
            processed = in.readLong();
            failed = in.readLong();
        }

        @Override
        public String toString() {
            return "[" + beanId + "], processed [" + processed + "], failed [" + failed + "]";
        }
    }


    public RemotingServiceMonitors() {
        super();
    }

    public RemotingServiceMonitors(String id, long processed, long failed, RemoteServiceStats[] remoteServiceStats) {
        super(id);
        getMonitors().put(Attributes.PROCESSED, processed);
        getMonitors().put(Attributes.FAILED, failed);
        for (RemoteServiceStats stats : remoteServiceStats) {
            getMonitors().put(stats.getBeanId(), stats);
        }
    }

    public RemotingServiceDetails getRemotingDetails() {
        return (RemotingServiceDetails) getDetails();
    }

    public long getProcessed() {
        return (Long) getMonitors().get(Attributes.PROCESSED);
    }

    public long getFailed() {
        return (Long) getMonitors().get(Attributes.FAILED);
    }

    public RemoteServiceStats getRemoteServiceStats(String id) {
        return (RemoteServiceStats) getMonitors().get(id);
    }

    public RemoteServiceStats[] getRemoteServiceStats() {
        ArrayList<RemoteServiceStats> remoteServiceStats = new ArrayList<RemoteServiceStats>();
        for (Object monitor : getMonitors().values()) {
            if (monitor instanceof RemoteServiceStats) {
                remoteServiceStats.add((RemoteServiceStats) monitor);
            }
        }
        return remoteServiceStats.toArray(new RemoteServiceStats[remoteServiceStats.size()]);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
}