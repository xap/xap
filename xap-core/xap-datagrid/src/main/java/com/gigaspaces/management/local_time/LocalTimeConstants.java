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

package com.gigaspaces.management.local_time;

import com.j_spaces.jmx.JMXProvider;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;

/**
 * @author evgenyf
 */
@com.gigaspaces.api.InternalApi
public class LocalTimeConstants {

    //logger
    final private static Logger _logger =
            LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ADMIN);


    static final String DOMAIN_NAME_SUFFIX = ".management";
    static final String OBJECT_TYPE = "LocalTime";
    static final String OBJECT_NAME = "time";

    final public static ObjectName MBEAN_NAME;

    static {
        ObjectName mbeanName = null;
        try {
            mbeanName = new ObjectName(JMXProvider.DEFAULT_DOMAIN +
                    DOMAIN_NAME_SUFFIX + ":type=" + OBJECT_TYPE + ",name=" + OBJECT_NAME);
        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.toString(), e);
            }
        }
        MBEAN_NAME = mbeanName;
    }


}
