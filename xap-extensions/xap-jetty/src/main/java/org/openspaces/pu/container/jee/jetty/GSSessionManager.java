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

import java.util.UUID;

import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.session.HashSessionManager;

/**
 * Our SessionManager implementation allows to change default sessionCookie value (JSESSIONID). fix
 * for GS-10830
 *
 * @author evgenyf
 * @since 9.7
 */
public class GSSessionManager extends HashSessionManager {

    @Override
    public void doStart() throws Exception {
        _sessionCookie = SessionManager.__DefaultSessionCookie + "_" + UUID.randomUUID();
        super.doStart();
    }
}
