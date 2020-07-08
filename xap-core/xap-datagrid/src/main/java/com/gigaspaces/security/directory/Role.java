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


package com.gigaspaces.security.directory;

import com.gigaspaces.security.Authority;
import com.gigaspaces.security.AuthorityFactory;

/**
 * Models role details retrieved by {@link RoleManager}. <p> Implementors may use this class
 * directly, subclass it, or write their own {@link RoleDetails} implementation from scratch. </p>
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class Role implements RoleDetails {

    private static final long serialVersionUID = 1L;
    private final String role;
    private final Authority[] authorities;

    public Role(String role) {
        this.role = role;
        this.authorities = new Authority[0];
    }

    public Role(String role, Authority... authorities) {
        this.role = role;
        this.authorities = authorities;
    }

    public Role(String role, String... authorities) {
        this.role = role;
        this.authorities = new Authority[authorities.length];
        for (int i = 0; i < authorities.length; ++i) {
            this.authorities[i] = AuthorityFactory.valueOf(authorities[i]);
        }
    }

    /*
     * @see com.gigaspaces.security.RoleDetails#getAuthorities()
     */
    public Authority[] getAuthorities() {
        return authorities;
    }

    /*
     * @see com.gigaspaces.security.RoleDetails#getRole()
     */
    public String getRole() {
        return role;
    }
}
