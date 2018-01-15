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

package com.gigaspaces.lrmi.nio;

import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.EmptyStackException;
import java.util.Stack;

/**
 * Created by IntelliJ IDEA. User: assafr Date: Oct 22, 2007 Time: 4:50:08 PM To change this
 * template use File | Settings | File Templates.
 */
@com.gigaspaces.api.InternalApi
public class TemporarySelectorFactory {

    /**
     * The number of <code>Selector</code> to create.
     */
    private final static int maxSelectors = Integer.getInteger(SystemProperties.LRMI_TMP_SELECTOR_POOL_SIZE,20);
    private final static long timeout = Long.getLong(SystemProperties.LRMI_TMP_SELECTOR_POOL_WAIT_TIMEOUT,5000);


    /**
     * Cache of <code>Selector</code>
     */
    private final static Stack<Selector> selectors = new Stack<Selector>();

    static {
        try {
            for (int i = 0; i < maxSelectors; i++)
                selectors.add(Selector.open());
        } catch (IOException ex) {
            // do nothing.
        }
    }


    /**
     * Get a exclusive <code>Selector</code>
     *
     * @return <code>Selector</code>
     */
    public final static Selector getSelector() {
        synchronized (selectors) {
            Selector s = null;
            try {
                if (selectors.size() != 0)
                    s = selectors.pop();
            } catch (EmptyStackException ex) {
            }

            int attempts = 0;
            try {
                while (s == null && attempts < 2) {
                    selectors.wait(timeout);
                    try {
                        if (selectors.size() != 0)
                            s = selectors.pop();
                    } catch (EmptyStackException ex) {
                        break;
                    }
                    attempts++;
                }
            } catch (InterruptedException ex) {
            }
            return s;
        }
    }


    /**
     * Return the <code>Selector</code> to the cache
     *
     * @param s <code>Selector</code>
     */
    public static void returnSelector(Selector s) {
        synchronized (selectors) {
            selectors.push(s);
            if (selectors.size() == 1)
                selectors.notify();
        }
    }


}
