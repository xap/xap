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

package com.gigaspaces.internal.utils.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * Thread factory to create {@link GSThread}.
 *
 * @author Guy
 * @see GSThread
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class GSThreadFactory implements ThreadFactory {

    private final boolean daemonThreads;
    private final String threadsName;

    public GSThreadFactory() {
        this(null, false);
    }

    public GSThreadFactory(String threadsName, boolean daemonThreads) {
        super();
        this.threadsName = threadsName;
        this.daemonThreads = daemonThreads;
    }

    public static GSThreadFactory daemon(String name) {
        return new GSThreadFactory(name, true);
    }

    public Thread newThread(Runnable r) {
        GSThread t = threadsName != null ? new GSThread(r, threadsName) : new GSThread(r);
        t.setDaemon(daemonThreads);
        return t;
    }
}
