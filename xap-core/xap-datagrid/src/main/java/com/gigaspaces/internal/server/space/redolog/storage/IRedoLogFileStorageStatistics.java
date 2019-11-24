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

package com.gigaspaces.internal.server.space.redolog.storage;

/**
 * Provide statistics of a {@link IRedoLogFileStorage}
 *
 * @author eitany
 * @since 7.1
 */
public interface IRedoLogFileStorageStatistics {

    /**
     * @return used space in bytes
     */
    long getSpaceUsed();

    /**
     * @return number of packets held outside of jvm memory
     */
    long getExternalPacketsCount();

    /**
     * @return number of packets held in jvm memory
     */
    long getMemoryPacketsCount();

    /**
     * @return total weight of packets held in memory
     */
    long getMemoryPacketsWeight();

    /**
     * @return total weight of packets held in external storage
     */
    long getExternalStoragePacketsWeight();

}