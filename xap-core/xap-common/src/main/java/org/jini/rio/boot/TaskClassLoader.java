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

package org.jini.rio.boot;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;

/**
 * Created by Barak Bar Orion A marker class. 7/1/15.
 */
@com.gigaspaces.api.InternalApi
public class TaskClassLoader extends URLClassLoader {
    public TaskClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    public TaskClassLoader(URL[] urls) {
        super(urls);
    }

    public TaskClassLoader(URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
        super(urls, parent, factory);
    }

    public RemoteClassLoaderInfo createRemoteClassLoaderInfo(){
        return new RemoteClassLoaderInfo(this.toString());
    }
}
