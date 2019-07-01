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

import java.util.concurrent.TimeUnit;


/**
 * Wraps a repetitive executing runnable which is provided to a {@link
 * IAsyncHandlerProvider#start(IAsyncCallable, long, String, boolean)} invocation and controls its
 * life cycle.
 *
 * @author eitany
 * @since 8.0
 */
public interface IAsyncHandler {
    /**
     * Wakes up the handler if in idle state and execute a cycle of the runnable
     */
    void wakeUp();

    /**
     * Wakes up the handler if in idle or suspended state and wait for it complete one cycle. Will
     * wait for the specified timeout until one cycle is completed and returns true if the cycle was
     * completed during that timeout, otherwise false.
     */
    boolean wakeUpAndWait(long timeout, TimeUnit units);

    /**
     * Stop the execution of this async handler. Will wait for the specified timeout until the
     * handler is stopped which guarantees no following execution of the wrapped runnable. If the
     * timeout elapsed this method returns but there might be an ongoing execution which was not
     * stopped yet. However that will be no new execution after this method returns
     */
    void stop(long timeout, TimeUnit units);

    /**
     * Resume the life cycle of the handler if it is in suspended mode, the next execution will be
     * scheduled according to the idle delay timeout the handler was created with
     *
     * @since 8.0.5
     */
    void resume();

    /**
     * Resume the life cycle of the handler if it is in suspended mode, the next execution will
     * start immediately.
     *
     * @since 8.0.5
     */
    void resumeNow();

    /**
     * Specifies whether this async handler is terminated and will not run again
     */
    boolean isTerminated();

}
