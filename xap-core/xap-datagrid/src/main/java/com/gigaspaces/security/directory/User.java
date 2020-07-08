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

import java.util.Arrays;

/**
 * Models user details retrieved by a {@link UserManager}. <p> Implementors may use this class
 * directly, subclass it, or write their own {@link UserDetails} implementation from scratch. </p>
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class User implements UserDetails {

    private static final long serialVersionUID = 1L;
    private final String username;
    private String password;
    private final Authority[] authorities;

    public User(String username, String password) {
        this.username = username;
        this.password = password;
        this.authorities = new Authority[0];
    }

    public User(String username, String password, Authority... authorities) {
        this.username = username;
        this.password = password;
        this.authorities = authorities;
    }

    public User(String username, String password, String... authorities) {
        this.username = username;
        this.password = password;
        this.authorities = new Authority[authorities.length];
        for (int i = 0; i < authorities.length; ++i) {
            this.authorities[i] = AuthorityFactory.valueOf(authorities[i]);
        }
    }

    /*
     * @see com.gigaspaces.security.UserDetails#getAuthorities()
     */
    public Authority[] getAuthorities() {
        return authorities;
    }

    /*
     * @see com.gigaspaces.security.UserDetails#getPassword()
     */
    public String getPassword() {
        return password;
    }

    /**
     * Set an encrypted password (new user or one with an updated password).
     *
     * @param encryptedPassword the encrypted password.
     * @see UserManager#createUser(UserDetails)
     * @see UserManager#updateUser(UserDetails)
     */
    public void setPassword(String encryptedPassword) {
        this.password = encryptedPassword;
    }

    /*
     * @see com.gigaspaces.security.UserDetails#getUsername()
     */
    public String getUsername() {
        return username;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(authorities);
        result = prime * result
                + ((password == null) ? 0 : password.hashCode());
        result = prime * result
                + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        User other = (User) obj;
        if (!Arrays.equals(authorities, other.authorities))
            return false;
        if (password == null) {
            if (other.password != null)
                return false;
        } else if (!password.equals(other.password))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }


}
