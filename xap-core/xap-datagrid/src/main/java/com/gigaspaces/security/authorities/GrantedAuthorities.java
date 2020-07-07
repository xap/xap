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


package com.gigaspaces.security.authorities;

import com.gigaspaces.security.Authority;
import com.gigaspaces.security.authorities.SpaceAuthority.Filter;
import com.gigaspaces.security.authorities.SpaceAuthority.NegateFilter;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates the authorities granted to a specific user, to ease access control and authorization
 * decisions.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public final class GrantedAuthorities implements Serializable {
    //implements Serializable since we return it to the proxy
    private static final long serialVersionUID = 1L;

    private final Map<Privilege, List<Authority>> authorityMap;
    private final Set<String> roles;

    public GrantedAuthorities(Authority[] authorities) {
        Map<Privilege, List<Authority>> map = new HashMap<Privilege, List<Authority>>();
        Set<String> rolesSet = new HashSet<String>();
        //normalize roles and authorities into one list
        List<Authority> normalizedAuthorities = new ArrayList<Authority>(authorities.length);
        for (Authority authority : authorities) {
            if (authority instanceof PopulatedRoleAuthority) {
                PopulatedRoleAuthority roleWrapper = (PopulatedRoleAuthority) authority;
                rolesSet.add(roleWrapper.getRole());
                for (Authority roleAuthority : roleWrapper.getAuthorities())
                    add(roleAuthority, normalizedAuthorities);
            } else {
                add(authority, normalizedAuthorities);
            }
        }

        for (Authority authority : normalizedAuthorities) {
            final Privilege key = ((InternalAuthority) authority).getPrivilege();
            List<Authority> list = map.get(key);
            if (list == null) {
                list = new ArrayList<Authority>(1);
            } else {
                list = new ArrayList<Authority>(list);
            }
            list.add(authority);
            map.put(key, Collections.unmodifiableList(list));
        }

        authorityMap = Collections.unmodifiableMap(map);
        roles = Collections.unmodifiableSet(rolesSet);
    }

    private static void add(Authority authority, List<Authority> authorities) {
        authorities.add(authority);
        if (authority instanceof SpaceAuthority && ((SpaceAuthority) authority).getPrivilege() == SpacePrivilege.WRITE)
            authorities.add(new SpaceAuthority(SpacePrivilege.CREATE, ((SpaceAuthority) authority).getFilter()));
    }

    /**
     * Evaluates to <code>true</code> if has been granted the required privilege, disregarding any
     * classname filters of a {@link SpaceAuthority}.
     *
     * @param privilege the required privilege.
     * @return <code>true</code> if the required privilege was granted; <code>false</code>
     * otherwise.
     */
    public boolean isGranted(Privilege privilege) {
        List<Authority> list = authorityMap.get(privilege);
        if (list == null) { //doesn't contain
            return false;
        } else {
            return true;
        }
    }

    /**
     * Evaluates to <code>true</code> if has been granted the required privileges to access the
     * specified object. The data object parameter is used against one of the {@link
     * SpaceAuthority.Filter}s; if <code>null</code> will return <code>false</code>. Currently all
     * implemented filters accept an object of type {@link String}.
     *
     * @param privilege the required privilege
     * @param object    the data object (can be null)
     * @return <code>true</code> if the required privilege to access the object was granted;
     * <code>false</code> otherwise.
     */
    public boolean isGranted(Privilege privilege, Object object) {
        List<Authority> list = authorityMap.get(privilege);
        if (list == null) { //doesn't contain
            return false;
        }

        if (!privilege.getClass().equals(SpacePrivilege.class)) {
            return true; //no filter to apply
        }

        Boolean include = null;
        Boolean exclude = null;
        for (Authority grantedAuthority : list) {
            SpaceAuthority authority = (SpaceAuthority) grantedAuthority;
            Filter filter = authority.getFilter();
            if (filter == null) {
                include = Boolean.TRUE;
                continue;
            }

            boolean accept = filter.accept(object);
            if (filter instanceof NegateFilter) {
                exclude = exclude == null ? Boolean.valueOf(accept) : exclude && accept;
            } else {
                include = include == null ? Boolean.valueOf(accept) : include || accept;
            }
        }

        include = include == null ? Boolean.TRUE : include;
        exclude = exclude == null ? Boolean.TRUE : exclude;
        return include && exclude;
    }

    /**
     * Returns a boolean indicating whether the authenticated user is included in the specified
     * logical "role".
     *
     * @param role a String specifying the name of the role
     * @return a boolean indicating whether the user making this request belongs to a given role;
     * false if the user has not been authenticated
     */
    public boolean isUserInRole(String role) {
        return roles.contains(role);
    }

    /**
     * @return all populated authorities belonging to this authenticated user
     * @since 15.5
     */
    public Authority[] getAuthorities() {
        ArrayList<Authority> list = new ArrayList<>();
        for (List<Authority> authorities : authorityMap.values()) {
            list.addAll(authorities);
        }
        return list.toArray(new Authority[0]);
    }
}
