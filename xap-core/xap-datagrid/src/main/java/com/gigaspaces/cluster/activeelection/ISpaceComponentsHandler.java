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


package com.gigaspaces.cluster.activeelection;

/**
 * Interface for classes that handle space components
 *
 * @author anna
 * @version 1.0
 * @since 5.1
 */
public interface ISpaceComponentsHandler

{
    /**
     * @param primaryOnly
     * @throws SpaceComponentsInitializeException
     */
    void initComponents(boolean primaryOnly) throws SpaceComponentsInitializeException;

    /**
     * @param primaryOnly
     */
    void startComponents(boolean primaryOnly);


    /**
     * Returns true if this ISpaceComponentsHandler can be enabled during space recovery, false
     * otherwise
     *
     * @return <code>true</code> if can be enabled during space recovery.
     */
    boolean isRecoverySupported();


    /**
     * @param primaryOnly
     */
    void close(boolean primaryOnly);

}
