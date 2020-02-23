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
 * A convenience class for retrieving the <code>boolean</code> value of a system property as a
 * privileged action.
 *
 * <p>An instance of this class can be used as the argument of {@link
 * AccessController#doPrivileged(PrivilegedAction) AccessController.doPrivileged} or {@link
 * Security#doPrivileged(PrivilegedAction) Security.doPrivileged}.
 *
 * <p>The following code retrieves the boolean value of the system property named
 * <code>"prop"</code> as a privileged action:
 *
 * <pre>
 * boolean b = ((Boolean) Security.doPrivileged(
 *                 new GetBooleanAction("prop"))).booleanValue();
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
public class GetBooleanAction implements PrivilegedAction {

    private static final Logger logger =
            Logger.getLogger("com.sun.jini.action.GetBooleanAction");

    private final String theProp;

    /**
     * Constructor that takes the name of the system property whose <code>boolean</code> value needs
     * to be determined.
     *
     * @param theProp the name of the system property
     **/
    public GetBooleanAction(String theProp) {
        this.theProp = theProp;
    }

    /**
     * Determines the <code>boolean</code> value of the system property whose name was specified in
     * the constructor.  The value is returned in a <code>Boolean</code> object.
     *
     * <p>If the system property is defined to equal the string <code>"true"</code> (case
     * insensitive), then this method returns a <code>Boolean</code> with the value
     * <code>true</code>.  Otherwise, this method returns a <code>Boolean</code> with the value
     * <code>false</code>.
     *
     * @return a <code>Boolean</code> representing the value of the system property
     **/
    public Object run() {
        try {
            return Boolean.getBoolean(theProp) ? Boolean.TRUE : Boolean.FALSE;
        } catch (SecurityException e) {
            if (logger.isLoggable(Level.FINE)) {
                LogUtil.logThrow(logger, Level.FINE,
                        GetBooleanAction.class, "run",
                        "security exception reading \"{0}\", returning false",
                        new Object[]{theProp}, e);
            }
            return Boolean.FALSE;
        }
    }
}
