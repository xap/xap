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
package org.openspaces.core.space.status;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.server.space.suspend.SuspendType;

/**
 * Interface receiving events when a space {@link SuspendType} or {@link SpaceMode} change
 *
 * @author Elad Gur
 * @since  14.0.1
 */
public interface SpaceStatusChangedEventListener {

    /**
     * @param event - an {@link SpaceStatusChangedEvent} that contain the {@link SuspendType} and the {@link SpaceMode}}
     */
    void onSpaceStatusChanged(SpaceStatusChangedEvent event);

}