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


package com.gigaspaces.admin.quiesce;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Represents the quiesce state of the all processing unit instances. Includes the following
 * information: The failed to quiesce/unquiesce instances with the reason of failure. The instances
 * which successfully quiesced/unquiesced. The number of missing instances which were planned but
 * not actually available while changing quiesce state.
 *
 * @author Boris
 * @since 10.1.0
 */
@com.gigaspaces.api.InternalApi
public class InstancesQuiesceState implements SmartExternalizable {

    private static final long serialVersionUID = 7886735384418998738L;

    private Set<String> quiescedInstances = new HashSet<String>();
    private Map<String, Throwable> failedToQuiesceInstances = new HashMap<String, Throwable>();
    private Integer missingInstancesCount;

    public InstancesQuiesceState() {
    }

    public InstancesQuiesceState(Integer missing) {
        this.missingInstancesCount = missing;
    }

    public Map<String, Throwable> getFailedToQuiesceInstances() {
        return failedToQuiesceInstances;
    }

    public Set<String> getQuiescedInstances() {
        return quiescedInstances;
    }

    public void addQuiesced(String name) {
        quiescedInstances.add(name);
    }

    public void addFailure(String name, Throwable e) {
        failedToQuiesceInstances.put(name, e);
    }

    public Integer getMissingInstancesCount() {
        return missingInstancesCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InstancesQuiesceState that = (InstancesQuiesceState) o;

        if (!failedToQuiesceInstances.equals(that.failedToQuiesceInstances)) return false;
        if (!quiescedInstances.equals(that.quiescedInstances)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = quiescedInstances.hashCode();
        result = 31 * result + failedToQuiesceInstances.hashCode();
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeStringSet(out, quiescedInstances);
        IOUtils.writeObject(out, failedToQuiesceInstances);
        IOUtils.writeObject(out, missingInstancesCount);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        quiescedInstances = IOUtils.readStringSet(in);
        failedToQuiesceInstances = IOUtils.readObject(in);
        missingInstancesCount = IOUtils.readObject(in);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (quiescedInstances.size() > 0) {
            sb.append("Succeeded instances: [ ");
            Iterator<String> quiescedInstancesIterator = quiescedInstances.iterator();
            while (quiescedInstancesIterator.hasNext()) {
                sb.append(quiescedInstancesIterator.next());
                if (quiescedInstancesIterator.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(" ]");
            sb.append('\n');
        }

        if (failedToQuiesceInstances.size() > 0) {
            sb.append("Failed instances: [ ");
            Iterator<String> failedToQuiesceInstancesIterator = failedToQuiesceInstances.keySet().iterator();
            while (failedToQuiesceInstancesIterator.hasNext()) {
                sb.append(failedToQuiesceInstancesIterator.next());
                if (failedToQuiesceInstancesIterator.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(" ]");
            sb.append('\n');
        }

        if (missingInstancesCount > 0) {
            sb.append("Missing number of instances: " + missingInstancesCount);
            sb.append('\n');
        }
        return sb.toString();
    }
}
