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

package org.openspaces.pu.container.servicegrid;

import com.gigaspaces.serialization.SmartExternalizable;
import net.jini.core.lookup.ServiceID;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.core.space.SpaceServiceDetails;
import org.openspaces.core.space.SpaceType;
import org.openspaces.pu.service.ServiceDetails;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author kimchy
 */
public class PUDetails implements SmartExternalizable {

    private static final long serialVersionUID = -6918314643571673741L;

    private ServiceID gscServiceID;

    private ClusterInfo clusterInfo;

    private BeanLevelProperties beanLevelProperties;

    private Object[] details;

    public PUDetails() {
    }

    public PUDetails(ServiceID gscServiceID, ClusterInfo clusterInfo, BeanLevelProperties beanLevelProperties, Object[] details) {
        this.gscServiceID = gscServiceID;
        this.clusterInfo = clusterInfo;
        this.beanLevelProperties = beanLevelProperties;
        this.details = details;
        if (details == null) {
            this.details = new ServiceDetails[0];
        }
    }

    public ServiceID getGscServiceID() {
        return gscServiceID;
    }

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    public BeanLevelProperties getBeanLevelProperties() {
        return beanLevelProperties;
    }

    public Object[] getDetails() {
        return this.details;
    }

    public SpaceServiceDetails getEmbeddedSpaceServiceDetails() {
        return getServiceDetails(SpaceServiceDetails.class, sd -> sd.getSpaceType() == SpaceType.EMBEDDED);
    }

    public SpaceServiceDetails getEmbeddedSpaceServiceDetails(ServiceID serviceID) {
        return getServiceDetails(SpaceServiceDetails.class, sd -> sd.getSpaceType() == SpaceType.EMBEDDED && sd.getServiceID().equals(serviceID));
    }

    public List<SpaceServiceDetails> getEmbeddedSpaceServiceDetailsList() {
        return getServiceDetailsList(SpaceServiceDetails.class, sd -> sd.getSpaceType() == SpaceType.EMBEDDED);
    }

    public <T> T getServiceDetails(Class<T> serviceType, Predicate<T> filter) {
        if (details != null) {
            for (Object detail : details) {
                if (serviceType.isInstance(detail)) {
                    T serviceDetails = serviceType.cast(detail);
                    if (filter == null || filter.test(serviceDetails))
                        return serviceDetails;
                }
            }
        }
        return null;
    }

    public <T> List<T> getServiceDetailsList(Class<T> serviceType, Predicate<T> filter) {
        List<T> result = new ArrayList<>();
        if (details != null) {
            for (Object detail : details) {
                if (serviceType.isInstance(detail)) {
                    T serviceDetails = serviceType.cast(detail);
                    if (filter == null || filter.test(serviceDetails))
                        result.add(serviceDetails);
                }
            }
        }
        return result;
    }

    /**
     * Return the name representing this Processing Unit (as shown in the UI).
     *
     * @return <tt>service-name</tt>.<tt>instance-id</tt> [<tt>backup-id</tt>] or
     * <tt>service-name</tt> [<tt>instance-id</tt>]
     */
    public String getPresentationName() {
        if (clusterInfo == null)
            return "null";
        Integer id = clusterInfo.getInstanceId();
        if (clusterInfo.getNumberOfBackups() == 0)
            return clusterInfo.getName() + " [" + id + "]";
        Integer bid = clusterInfo.getBackupId();
        if (bid == null)
            bid = Integer.valueOf(0);
        return clusterInfo.getName() + "." + id + " [" + (bid + 1) + "]";
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(gscServiceID);
        out.writeObject(clusterInfo);
        out.writeInt(details.length);
        for (Object details : this.details) {
            out.writeObject(details);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        gscServiceID = (ServiceID) in.readObject();
        clusterInfo = (ClusterInfo) in.readObject();
        int size = in.readInt();
        this.details = new ServiceDetails[size];
        for (int i = 0; i < size; i++) {
            details[i] = in.readObject();
        }
    }

    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }
}
