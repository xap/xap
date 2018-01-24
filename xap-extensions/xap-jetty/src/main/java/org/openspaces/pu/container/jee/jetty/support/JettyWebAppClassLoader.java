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

package org.openspaces.pu.container.jee.jetty.support;

import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jini.rio.boot.LoggableClassLoader;

import java.io.IOException;

/**
 * A simple extension for Jetty WebApp class loader just to make it loggable in the Service Grid.
 *
 * @author kimchy
 */
public class JettyWebAppClassLoader extends WebAppClassLoader implements LoggableClassLoader {

    private final String loggableName;

    public JettyWebAppClassLoader(ClassLoader parent, WebAppContext context, String loggableName) throws IOException {
        super(parent, context);
        this.loggableName = loggableName;
    }

    public String getLogName() {
        return loggableName;
    }
}
