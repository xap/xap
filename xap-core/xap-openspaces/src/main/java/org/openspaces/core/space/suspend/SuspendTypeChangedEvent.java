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
package org.openspaces.core.space.suspend;

import com.gigaspaces.server.space.suspend.SuspendType;
import com.j_spaces.core.IJSpace;
import org.springframework.context.ApplicationEvent;

public class SuspendTypeChangedEvent extends ApplicationEvent {

    private static final long serialVersionUID = 1L;

    private SuspendType suspendType;

    /**
     * Creates a new Space suspend type changed event.
     *
     * @param space     The space that changed its suspend type
     * @param suspendType The current suspend type of the space
     */
    public SuspendTypeChangedEvent(IJSpace space, SuspendType suspendType) {
        super(space);
        this.suspendType = suspendType;
    }

    /**
     * Returns the space that initiated this event.
     */
    public IJSpace getSpace() {
        return (IJSpace) getSource();
    }

    public SuspendType getSuspendType() {
        return this.suspendType;
    }

}
