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
import org.openspaces.pu.service.PlainServiceDetails;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

/**
 * A generic remoting service details.
 *
 * @author kimchy
 */
public class RemotingServiceDetails extends PlainServiceDetails {

    private static final long serialVersionUID = 5538531964331522954L;

    public static final String SERVICE_TYPE = "remoting";

    public static class RemoteService implements SmartExternalizable {

        private static final long serialVersionUID = -8527660067530155L;

        private String beanId;
        private String className;

        public RemoteService() {
        }

        public RemoteService(String beanId, String className) {
            this.beanId = beanId;
            this.className = className;
        }

        public String getBeanId() {
            return beanId;
        }

        public String getClassName() {
            return className;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(beanId);
            out.writeObject(className);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            beanId = (String) in.readObject();
            className = (String) in.readObject();
        }

        @Override
        public String toString() {
            return "[" + beanId + "], class [" + className + "]";
        }
    }

    public static class Attributes {
    }

    public RemotingServiceDetails() {
        super();
    }

    public RemotingServiceDetails(String id, RemoteService[] remoteServices) {
        super(id, SERVICE_TYPE, "", "Remoting Service", "Remoting Service");
        for (RemoteService service : remoteServices) {
            getAttributes().put(service.getBeanId(), service);
        }
    }

    public RemoteService getRemoteService(String id) {
        return (RemoteService) getAttributes().get(id);
    }

    public RemoteService[] getRemoteServices() {
        ArrayList<RemoteService> remoteServices = new ArrayList<RemoteService>();
        for (Object attr : getAttributes().values()) {
            if (attr instanceof RemoteService) {
                remoteServices.add((RemoteService) attr);
            }
        }
        return remoteServices.toArray(new RemoteService[remoteServices.size()]);
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