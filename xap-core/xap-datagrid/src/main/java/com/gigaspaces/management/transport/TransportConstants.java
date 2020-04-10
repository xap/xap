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

package com.gigaspaces.management.transport;

import com.j_spaces.jmx.JMXProvider;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * @author evgenyf
 */
@com.gigaspaces.api.InternalApi
public class TransportConstants {

    //logger
    final private static Logger _logger =
            LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ADMIN);


    static final String DOMAIN_NAME_SUFFIX = ".management";
    static final String OBJECT_TYPE = "TransportConnections";
    static final String OBJECT_NAME = "transport";

    private static String MBEAN_NAME = JMXProvider.DEFAULT_DOMAIN +
            DOMAIN_NAME_SUFFIX + ":type=" + OBJECT_TYPE + ",name=" + OBJECT_NAME;

    private static Map<String, ObjectName> objectsNameMap = new HashMap<String, ObjectName>();


    public static ObjectName createTransportMBeanObjectName(String containerName) {
        ObjectName objectName = objectsNameMap.get(containerName);
        if (objectName == null) {
            try {
                objectName = new ObjectName(MBEAN_NAME + "-" + containerName);
                objectsNameMap.put(containerName, objectName);
            } catch (MalformedObjectNameException exc) {
                _logger.error(exc.toString(), exc);
            }
        }

        return objectName;
    }

}
