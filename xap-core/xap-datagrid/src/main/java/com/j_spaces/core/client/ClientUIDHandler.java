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

package com.j_spaces.core.client;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.internal.server.space.SpaceUidFactory;

/**
 * This Class enables the client to create entry UIDs by rendering a free-string , and to extract
 * the free-string from a UID.
 *
 * @author Yechiel Fefer
 * @version 3.2
 * @deprecated Since 8.0 - This class is reserved for internal usage only, Use {@link SpaceId}
 * annotation instead.
 */
@Deprecated

public class ClientUIDHandler {
    private ClientUIDHandler() {
    }

    /**
     * This method is used to create a UID given a name (i.e. any free string) and the entry (full)
     * classname. NOTE - it is the caller's responsibility to create a unique UID by supplying a
     * unique name
     *
     * @param name     any free string; <p>The following characters are not allowed as part of the
     *                 entry name: <b>! @ # $ % ^ & * ( ) _ + = - ? > < , . / " : ; ' | \ } { [ ]
     *                 ^</b> </p>
     * @param typeName full class-name string
     * @return UID string
     */
    public static String createUIDFromName(Object name, String typeName) {
        if (name == null)
            throw new RuntimeException("CreateUIDFromName: a non-null object must be supplied for name.");
        if (typeName == null)
            throw new RuntimeException("CreateUIDFromName: a non-null string must be supplied for className.");

        return SpaceUidFactory.createUidFromTypeAndId(typeName, name.toString());
    }
}
