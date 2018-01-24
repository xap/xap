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


package org.openspaces.jee.sessions.jetty;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.session.AbstractSession;
import org.eclipse.jetty.server.session.AbstractSessionManager;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.webapp.WebAppContext;
import org.openspaces.core.GigaSpace;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * GigaspacesSessionIdManager
 *
 * A Jetty SessionIDManager where the in-use session ids are stored in a data grid "cloud".
 */
public class GigaSessionIdManager extends AbstractSessionIdManager {

    protected final Server _server;

    protected GigaSpace gigaSpace;

    public GigaSessionIdManager(Server server) {
        this._server = server;
    }

    public void setSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    public GigaSpace getSpace() {
        return gigaSpace;
    }

    /**
     * Start up the id manager.
     */
    public void doStart() {
        try {
            if (gigaSpace == null) {
                initSpace();
            }
            super.doStart();
        } catch (Exception e) {
            Log.getLog().warn("Problem initialising session ids", e);
            throw new IllegalStateException(e);
        }
    }

    protected void initSpace() throws Exception {
        if (gigaSpace == null) {
            throw new IllegalStateException("No space configured");
        }
    }

    public void addSession(HttpSession session) {
        // do nothing, we use SessionData
    }

    public void removeSession(HttpSession session) {
        // do nothing, we reused the SessionData
    }

    @Override
    public void renewSessionId(String oldClusterId, String oldNodeId, HttpServletRequest request) {
        try {
            SessionData sessionData = gigaSpace.takeById(SessionData.newIdQuery(oldClusterId));
            if (sessionData == null) {
                if (Log.getLog().isDebugEnabled())
                    Log.getLog().debug("Cannot renew session Id [" + oldClusterId + "] - does not exist in space");
            } else {
                String newClusterId = getClusterId(request.getRequestedSessionId());
                sessionData.setId(newClusterId);
                gigaSpace.write(sessionData);
                if (Log.getLog().isDebugEnabled())
                    Log.getLog().debug("Renewed session Id [" + oldClusterId + "] to [" + newClusterId + "]");
            }
        } catch (Exception e) {
            Log.getLog().warn("Failed to renew [oldClusterId=" + oldClusterId + ", oldNodeId=" + oldNodeId + "]", e);
        }
    }

    public String getClusterId(String nodeId) {
        int dot = nodeId.lastIndexOf('.');
        return (dot > 0) ? nodeId.substring(0, dot) : nodeId;
    }

    public String getNodeId(String clusterId, HttpServletRequest request) {
        return _workerName != null ? clusterId + '.' + _workerName : clusterId;
    }

    @Override
    public boolean idInUse(String id) {
        if (id == null)
            return false;

        final String sessionId = getClusterId(id);
        try {
            int count = gigaSpace.count(SessionData.newIdQuery(sessionId));
            boolean exists = count > 0;
            if (Log.getLog().isDebugEnabled())
                Log.getLog().debug("Id [" + id + "] " + (exists ? "exists" : "does not exist") + " in space");
            return exists;
        } catch (Exception e) {
            Log.getLog().warn("Problem checking inUse for id=" + sessionId, e);
            return false;
        }
    }

    public void invalidateAll(String id) {
        //tell all contexts that may have a session object with this id to
        //get rid of them
        Handler[] contexts = _server.getChildHandlersByClass(WebAppContext.class);
        for (int i = 0; contexts != null && i < contexts.length; i++) {
            AbstractSessionManager manager = ((AbstractSessionManager) ((WebAppContext) contexts[i]).getSessionHandler().getSessionManager());
            AbstractSession session = manager.getSession(id);
            if (session != null) {
                session.invalidate();
            }
        }
    }
}
