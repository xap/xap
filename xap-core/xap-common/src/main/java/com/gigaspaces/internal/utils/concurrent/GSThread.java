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


/**
 * Base thread that should be used all across GigaSpaces code. Add "GS-" prefix to all the threads
 * names.
 *
 * @author Guy
 * @see GSThreadFactory
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class GSThread extends Thread {
    private static final String PREFIX = "GS-";

    public GSThread(Runnable target) {
        super(target);
        setName(PREFIX + getName());
    }

    public GSThread(String name) {
        super(appendPrefixIfNeeded(name));
    }

    public GSThread(Runnable target, String name) {
        super(target, appendPrefixIfNeeded(name));
    }

    public GSThread(ThreadGroup group, Runnable target, String name) {
        super(group, target, appendPrefixIfNeeded(name));
    }

    public GSThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, target, appendPrefixIfNeeded(name), stackSize);
    }

    public static GSThread daemon(Runnable target, String name) {
        GSThread thread = new GSThread(target, name);
        thread.setDaemon(true);
        return thread;
    }

    private static String appendPrefixIfNeeded(String name) {
        return name.startsWith(PREFIX) ? name : PREFIX + name;
    }
}
