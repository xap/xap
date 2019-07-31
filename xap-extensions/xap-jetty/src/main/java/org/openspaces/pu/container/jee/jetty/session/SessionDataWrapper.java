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

import com.gigaspaces.annotation.pojo.SpaceClassConstructor;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import org.eclipse.jetty.server.session.SessionData;

/**
 * @since 15.0.0
 * @author Niv Ingberg
 */
public class SessionDataWrapper {
    private final String id;
    private final SessionData sessionData;

    @SpaceClassConstructor
    public SessionDataWrapper(String id, SessionData sessionData) {
        this.id = id;
        this.sessionData = sessionData;
    }

    @SpaceId
    public String getId() {
        return id;
    }

    @SpaceIndex(path = "id")
    public SessionData getSessionData() {
        return sessionData;
    }
}
