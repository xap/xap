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
import com.gigaspaces.internal.io.MarshObjectConvertor;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private static final SpaceUidFactory _factory = new SpaceUidFactory();
    private static final Method DUMMY_METHOD;
    private static final Map<Class<?>, Method> GET_UID_MAP = new ConcurrentHashMap<Class<?>, Method>();
    private static final String GET_UID_METHOD_NAME = "__getUID";
    private static final Class<?>[] GET_UID_METHOD_NAME_PARAM = new Class[0];
    private static final Object[] GET_UID_METHOD_NAME_ARGS = new Object[0];
    private static final boolean _serUID = Boolean.getBoolean(SystemProperties.SER_UID);
    private static final MarshObjectConvertor mc = new MarshObjectConvertor();

    static {
        try {
            DUMMY_METHOD = Object.class.getMethod("toString", new Class[0]);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

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

        String basicName;
        boolean validate;
        if (_serUID) {
            try {
                basicName = new String(mc.toBinary(name), "ISO-8859-1");
                validate = false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            basicName = getUID(name);
            validate = true;
        }

        final String uid = _factory.createUidFromTypeAndId(typeName, basicName, validate);
        return uid.length() == 0 ? null : uid;
    }

    private static String getUID(Object name) {
        if (name instanceof String)
            return (String) name;

        Class<?> cls = name.getClass(); // Looking for __getUID()
        Method method = GET_UID_MAP.get(cls);
        if (method == null) {
            try {
                method = cls.getMethod(GET_UID_METHOD_NAME, GET_UID_METHOD_NAME_PARAM);
            } catch (NoSuchMethodException e) {
                method = DUMMY_METHOD;
            }
            GET_UID_MAP.put(cls, method);
        }

        if (method == DUMMY_METHOD)
            return name.toString();

        try {
            return (String) method.invoke(name, GET_UID_METHOD_NAME_ARGS);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } // Strange error can't really happen
        catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}