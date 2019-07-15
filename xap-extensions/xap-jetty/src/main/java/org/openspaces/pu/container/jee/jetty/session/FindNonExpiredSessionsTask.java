/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package org.openspaces.pu.container.jee.jetty.session;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.j_spaces.core.client.SQLQuery;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.TaskGigaSpace;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @since 15.0.0
 * @author Niv Ingberg
 */
public class FindNonExpiredSessionsTask implements DistributedTask<HashSet<String>,HashSet<String>>, Externalizable {

    private static final long serialVersionUID = 1L;

    @TaskGigaSpace
    private transient GigaSpace gigaSpace;
    private Set<String> candidates;
    private String contextPath;
    private String vhost;
    private long managedExpiry;
    private long unmanagedExpiry;

    /** Required for @{@link Externalizable} */
    public FindNonExpiredSessionsTask() {
    }

    public FindNonExpiredSessionsTask(Set<String> candidates, String contextPath, String vhost,
                                      long managedExpiry, long unmanagedExpiry) {
        this.candidates = candidates;
        this.contextPath = contextPath;
        this.vhost = vhost;
        this.managedExpiry = managedExpiry;
        this.unmanagedExpiry = unmanagedExpiry;
    }

    @Override
    public HashSet<String> execute() throws Exception {
        HashSet<String> result = new HashSet<>();
        // Get all collocated candidates (sessionData.id is indexed):
        SpaceSessionData[] sessions = gigaSpace.readMultiple(new SQLQuery<>(SpaceSessionData.class,
                "id IN (?)", candidates));
        // Check if expired, collect non-expired:
        for (SpaceSessionData session : sessions) {
            if (!isExpired(session))
                result.add(session.getId());
        }

        return result;
    }

    private boolean isExpired(SpaceSessionData session) {
        if (session.getExpiry() <= 0)
            return false;

        long expiry = contextPath.equals(session.getContextPath()) && vhost.equals(session.getVhost()) ?
                managedExpiry : unmanagedExpiry;
        return session.getExpiry() <= expiry;
    }

    @Override
    public HashSet<String> reduce(List<AsyncResult<HashSet<String>>> asyncResults) throws Exception {
        HashSet<String> result = new HashSet<>();
        for (AsyncResult<HashSet<String>> asyncResult : asyncResults) {
            if (asyncResult.getException() != null) {
                throw asyncResult.getException();
            } else {
                result.addAll(asyncResult.getResult());
            }
        }

        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeStringSet(out, candidates);
        IOUtils.writeString(out, contextPath);
        IOUtils.writeString(out, vhost);
        out.writeLong(managedExpiry);
        out.writeLong(unmanagedExpiry);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.candidates = IOUtils.readStringSet(in);
        this.contextPath = IOUtils.readString(in);
        this.vhost = IOUtils.readString(in);
        this.managedExpiry = in.readLong();
        this.unmanagedExpiry = in.readLong();
    }
}
