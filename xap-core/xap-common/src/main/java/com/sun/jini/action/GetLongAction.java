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

package com.sun.jini.action;

import com.sun.jini.logging.LogUtil;

import net.jini.security.Security;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A convenience class for retrieving the <code>long</code> value of a system property as a
 * privileged action.
 *
 * <p>An instance of this class can be used as the argument of {@link
 * AccessController#doPrivileged(PrivilegedAction) AccessController.doPrivileged} or {@link
 * Security#doPrivileged(PrivilegedAction) Security.doPrivileged}.
 *
 * <p>The following code retrieves the <code>long</code> value of the system property named
 * <code>"prop"</code> as a privileged action. Since it does not pass a default value to be used in
 * case the property <code>"prop"</code> is not defined, it has to check the result for
 * <code>null</code>:
 *
 * <pre>
 * Long tmp = (Long)
 *     Security.doPrivileged(new GetLongAction("prop"));
 * long l;
 * if (tmp != null) {
 *     l = tmp.longValue();
 * }
 * </pre>
 *
 * <p>The following code retrieves the <code>long</code> value of the system property named
 * <code>"prop"</code> as a privileged action, and also passes a default value to be used in case
 * the property <code>"prop"</code> is not defined:
 *
 * <pre>
 * long l = ((Long) Security.doPrivileged(
 *                 new GetLongAction("prop"))).longValue();
 * </pre>
 *
 * <p>If the protection domain of the immediate caller of <code>doPrivileged</code> or the
 * protection domain of this class does not imply the permissions necessary for the operation, the
 * behavior is as if the system property is not defined.
 *
 * @author Sun Microsystems, Inc.
 * @see PrivilegedAction
 * @see AccessController
 * @see Security
 * @since 2.0
 **/
@com.gigaspaces.api.InternalApi
public class GetLongAction implements PrivilegedAction {

    private static final Logger logger =
            Logger.getLogger("com.sun.jini.action.GetLongAction");

    private final String theProp;
    private final long defaultVal;
    private final boolean defaultSet;

    /**
     * Constructor that takes the name of the system property whose <code>long</code> value needs to
     * be determined.
     *
     * @param theProp the name of the system property
     **/
    public GetLongAction(String theProp) {
        this.theProp = theProp;
        defaultVal = 0;
        defaultSet = false;
    }

    /**
     * Constructor that takes the name of the system property and the default value of that
     * property.
     *
     * @param theProp    the name of the system property
     * @param defaultVal the default value
     **/
    public GetLongAction(String theProp, long defaultVal) {
        this.theProp = theProp;
        this.defaultVal = defaultVal;
        defaultSet = true;
    }

    /**
     * Determines the <code>long</code> value of the system property whose name was specified in the
     * constructor.  The value is returned in a <code>Long</code> object.
     *
     * <p>If the system property is defined to be a value that can be parsed successfully by {@link
     * Long#decode Long.decode}, then this method returns an <code>Long</code> with the parsed
     * value. Otherwise, if a default value was supplied to this object's constructor, then this
     * method returns an <code>Long</code> with that default value, or else <code>null</code> is
     * returned.
     *
     * @return a <code>Long</code> representing the value of the system property or the default
     * value, or <code>null</code>
     **/
    public Object run() {
        try {
            Long value = Long.getLong(theProp);
            if (value != null) {
                return value;
            }
        } catch (SecurityException e) {
            if (logger.isLoggable(Level.FINE)) {
                LogUtil.logThrow(logger, Level.FINE,
                        GetLongAction.class, "run",
                        "security exception reading \"{0}\", returning {1}",
                        new Object[]{theProp, defaultValue()}, e);
            }
        }
        return defaultValue();
    }

    private Long defaultValue() {
        return defaultSet ? new Long(defaultVal) : null;
    }
}
