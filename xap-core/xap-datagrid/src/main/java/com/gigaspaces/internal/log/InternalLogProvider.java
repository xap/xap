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

package com.gigaspaces.internal.log;

import com.gigaspaces.admin.ManagerClusterInfo;
import com.gigaspaces.annotation.lrmi.MonitoringPriority;
import com.gigaspaces.log.LogEntries;
import com.gigaspaces.log.LogEntryMatcher;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author kimchy
 */
public interface InternalLogProvider extends Remote {

    @MonitoringPriority
    LogEntries logEntriesDirect(LogEntryMatcher matcher) throws RemoteException, IOException;

    /**
     * Returns cluster manager info for relevant services.
     * NOTE: This method was added here to preserve backwards compatibility (adding a new interface breaks backwards).
     * It is only implemented by GigaRegistrar.
     * @since 15.5
     */
    default ManagerClusterInfo getManagerClusterInfo() throws RemoteException {
        throw new UnsupportedOperationException();
    }
}
