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

/*
 * @(#)FiltersInfo.java 1.0   28/12/2002  9:59AM
 */

package com.j_spaces.core.filters;

import java.io.Serializable;
import java.util.Vector;

/**
 * This class contains all information about space filter configuration. This structure builds
 * inside of server.
 *
 * @author Igor Goldenberg
 * @version 1.2
 */
@com.gigaspaces.api.InternalApi
public class FiltersInfo
        implements Serializable, Cloneable {
    /**
     * use serialVersionUID from GigaSpaces 3.0 for interoperability.
     */
    static final long serialVersionUID = 6597126597902489582L;

    public String filterName;
    public String filterClassName;
    public String paramURL;
    public boolean enabled = true;

    public boolean beforeNotify;
    public boolean beforeWrite;
    public boolean beforeRead;
    public boolean beforeTake;
    public boolean afterRead;
    public boolean afterTake;

    public boolean beforeClean;
    public boolean afterRemove;
    public boolean afterWrite;
    public boolean afterUpdate;
    public boolean beforeUpdate;
    public boolean beforeGetAdmin;
    public boolean beforeAuthentication;

    // TODO OLD SECURITY - NEED TO REMOVE IN NEXT MAJOR VERSION AFTER 7.0
    @Deprecated public boolean secured;             // Unused, maintained for backwards because class is Serializable
    @Deprecated public Object[] usersInfo;          // Unused, maintained for backwards because class is Serializable
    @Deprecated public Object[] userDefinedRoles;   // Unused, maintained for backwards because class is Serializable

    @Override
    public String toString() {
        return filterName;
    }

    /**
     * Creates and returns a copy of this object.
     *
     * @see java.lang.Object#clone()
     */
    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            // this shouldn't happen, since we are not Cloneable
            throw new InternalError();
        }
    }
}