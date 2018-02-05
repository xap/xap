/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.config;

/**
 * A set of static convenience methods for use in configuration files. This class cannot be
 * instantiated.
 *
 * @author Sun Microsystems, Inc.
 * @see net.jini.config.ConfigurationFile
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class ConfigUtil {
    /**
     * This class cannot be instantiated.
     */
    private ConfigUtil() {
        throw new AssertionError(
                "com.sun.jini.config.ConfigUtil cannot be instantiated");
    }

    /**
     * Concatenate the strings resulting from calling {@link java.lang.String#valueOf(Object)} on
     * each element of an array of objects. Passing a zero length array will result in the empty
     * string being returned.
     *
     * @param objects the array of objects to be processed.
     * @return the concatenation of the return values from calling <code>String.valueOf</code> on
     * each element of <code>objects</code>.
     * @throws NullPointerException if <code>objects</code> is <code>null</code>.
     */
    public static String concat(Object[] objects) {
        if (objects.length == 0)
            return "";

        final StringBuffer buf = new StringBuffer(String.valueOf(objects[0]));
        for (int i = 1; i < objects.length; i++)
            buf.append(objects[i]);

        return buf.toString();
    }
}
