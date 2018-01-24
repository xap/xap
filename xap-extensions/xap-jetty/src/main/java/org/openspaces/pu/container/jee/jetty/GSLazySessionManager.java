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

package org.openspaces.pu.container.jee.jetty;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.http.HttpCookie;
import org.eclipse.jetty.server.SessionIdManager;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.session.HashSessionManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.util.annotation.ManagedAttribute;
import org.eclipse.jetty.util.component.LifeCycle;

import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import java.util.ArrayList;
import java.util.EventListener;
import java.util.List;
import java.util.Set;

/**
 * 7/8/2014. Created by Barak Bar Orion.
 */
@SuppressWarnings("UnusedDeclaration")
public class GSLazySessionManager implements LifeCycle, SessionManager {

    private static final Log logger = LogFactory.getLog(GSLazySessionManager.class);

    private volatile SessionManager defaultSessionManager;
    private volatile boolean replaced;
    private SessionHandler handler;
    private List<EventListener> savedEventListeners;

    public GSLazySessionManager() {
        this(new HashSessionManager());
    }

    public GSLazySessionManager(SessionManager defaultSessionManager) {
        this.defaultSessionManager = defaultSessionManager;
        this.replaced = false;
        savedEventListeners = new ArrayList<EventListener>();
    }

    public void replaceDefault(SessionManager sessionManager) throws Exception {
        if (sessionManager == null || replaced) {
            return;
        }
        replaced = true;
        if (defaultSessionManager != null) {
            defaultSessionManager.stop();
        }
        defaultSessionManager = sessionManager;
        defaultSessionManager.setSessionHandler(handler);
        for (EventListener savedEventListener : savedEventListeners) {
            defaultSessionManager.addEventListener(savedEventListener);
        }
        if (!defaultSessionManager.isStarted() || defaultSessionManager.isRunning()) {
            defaultSessionManager.start();
        }
    }

    @ManagedAttribute("true if cookies use the http only flag")
    public boolean getHttpOnly() {
        return defaultSessionManager.getHttpOnly();
    }

    public void addEventListener(EventListener listener) {
        defaultSessionManager.addEventListener(listener);
        if (!replaced) {
            savedEventListeners.add(listener);
        }
    }

    public HttpSession getHttpSession(String nodeId) {
        return defaultSessionManager.getHttpSession(nodeId);
    }

    public SessionCookieConfig getSessionCookieConfig() {
        return defaultSessionManager.getSessionCookieConfig();
    }

    public String getSessionIdPathParameterNamePrefix() {
        return defaultSessionManager.getSessionIdPathParameterNamePrefix();
    }

    public boolean isValid(HttpSession session) {
        return defaultSessionManager.isValid(session);
    }

    public boolean isFailed() {
        return defaultSessionManager.isFailed();
    }

    public boolean isStopping() {
        return defaultSessionManager.isStopping();
    }

    @Deprecated
    public SessionIdManager getMetaManager() {
        //noinspection deprecation
        return defaultSessionManager.getMetaManager();
    }

    public void addLifeCycleListener(LifeCycle.Listener listener) {
        defaultSessionManager.addLifeCycleListener(listener);
    }

    public void setCheckingRemoteSessionIdEncoding(boolean remote) {
        defaultSessionManager.setCheckingRemoteSessionIdEncoding(remote);
    }

    public void setSessionIdManager(SessionIdManager metaManager) {
        defaultSessionManager.setSessionIdManager(metaManager);
    }

    public HttpSession newHttpSession(HttpServletRequest request) {
        return defaultSessionManager.newHttpSession(request);
    }

    public Set<SessionTrackingMode> getDefaultSessionTrackingModes() {
        return defaultSessionManager.getDefaultSessionTrackingModes();
    }

    public String getClusterId(HttpSession session) {
        return defaultSessionManager.getClusterId(session);
    }

    public boolean isStarted() {
        return defaultSessionManager.isStarted();
    }

    public void removeLifeCycleListener(LifeCycle.Listener listener) {
        defaultSessionManager.removeLifeCycleListener(listener);
    }

    public void stop() throws Exception {
        defaultSessionManager.stop();
    }

    @ManagedAttribute("check remote session id encoding")
    public boolean isCheckingRemoteSessionIdEncoding() {
        return defaultSessionManager.isCheckingRemoteSessionIdEncoding();
    }

    public void renewSessionId(String oldClusterId, String oldNodeId, String newClusterId, String newNodeId) {
        defaultSessionManager.renewSessionId(oldClusterId, oldNodeId, newClusterId, newNodeId);
    }


    public String getNodeId(HttpSession session) {
        return defaultSessionManager.getNodeId(session);
    }

    public boolean isRunning() {
        return defaultSessionManager.isRunning();
    }

    @ManagedAttribute("name of use for URL session tracking")
    public String getSessionIdPathParameterName() {
        return defaultSessionManager.getSessionIdPathParameterName();
    }

    @ManagedAttribute("Session ID Manager")
    public SessionIdManager getSessionIdManager() {
        return defaultSessionManager.getSessionIdManager();
    }

    public void setSessionTrackingModes(Set<SessionTrackingMode> sessionTrackingModes) {
        defaultSessionManager.setSessionTrackingModes(sessionTrackingModes);
    }

    public void start() throws Exception {
        defaultSessionManager.start();
    }

    public void complete(HttpSession session) {
        defaultSessionManager.complete(session);
    }

    public boolean isStopped() {
        return defaultSessionManager.isStopped();
    }

    public boolean isStarting() {
        return defaultSessionManager.isStarting();
    }

    public boolean isUsingURLs() {
        return defaultSessionManager.isUsingURLs();
    }

    public void setMaxInactiveInterval(int seconds) {
        defaultSessionManager.setMaxInactiveInterval(seconds);
    }

    public void clearEventListeners() {
        defaultSessionManager.clearEventListeners();
    }

    public void setSessionIdPathParameterName(String param) {
        defaultSessionManager.setSessionIdPathParameterName(param);
    }

    public void removeEventListener(EventListener listener) {
        defaultSessionManager.removeEventListener(listener);
        if (!replaced) {
            savedEventListeners.remove(listener);
        }
    }

    public boolean isUsingCookies() {
        return defaultSessionManager.isUsingCookies();
    }

    public HttpCookie getSessionCookie(HttpSession session, String contextPath, boolean requestIsSecure) {
        return defaultSessionManager.getSessionCookie(session, contextPath, requestIsSecure);
    }

    public void setSessionHandler(SessionHandler sessionHandler) {
        this.handler = sessionHandler;
        defaultSessionManager.setSessionHandler(sessionHandler);
    }

    public HttpCookie access(HttpSession session, boolean secure) {
        return defaultSessionManager.access(session, secure);
    }

    @ManagedAttribute("defailt maximum time a session may be idle for (in s)")
    public int getMaxInactiveInterval() {
        return defaultSessionManager.getMaxInactiveInterval();
    }

    public Set<SessionTrackingMode> getEffectiveSessionTrackingModes() {
        return defaultSessionManager.getEffectiveSessionTrackingModes();
    }
}
