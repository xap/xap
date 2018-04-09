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

package net.jini.discovery;

import java.io.IOException;

/**
 * This interface defines methods and constants related to the management of the sets of lookup
 * services that are to be discovered using the multicast discovery protocol; that is, lookup
 * services that are discovered by way of group discovery. The methods of this interface define how
 * an entity retrieves or modifies the set of groups associated with those lookup services.
 *
 * @author Sun Microsystems, Inc.
 */
public interface DiscoveryGroupManagement {

    /**
     * Returns an array consisting of the elements of the managed set of groups; that is, the names
     * of the groups whose members are the lookup services to discover. If the managed set of groups
     * is empty, this method will return the empty array. If there is no managed set of groups, then
     * null is returned; indicating that all groups are to be discovered. This method returns a new
     * array upon each invocation.
     *
     * @return <code>String</code> array consisting of the elements of the managed set of groups
     * @see #setGroups
     */
    public String[] getGroups();

    /**
     * Adds a set of group names to the managed set of groups. Elements in the input set that
     * duplicate elements already in the managed set will be ignored. Once a new name is added to
     * the managed set, attempts will be made to discover all (as yet) undiscovered lookup services
     * that are members of the group having that name. If the empty array (NO_GROUPS) is input, the
     * managed set of groups will not change.
     *
     * Note that any entity that invokes this method must have <code>DiscoveryPermission</code> on
     * each of the groups in the new set, otherwise a <code>SecurityException</code> will be
     * propagated through this method.
     *
     * @param groups <code>String</code> array consisting of the group names to add to the managed
     *               set.
     * @throws java.io.IOException                     because an invocation of this method may
     *                                                 result in the re-initiation of the discovery
     *                                                 process, which can throw an <code>IOException</code>
     *                                                 when socket allocation occurs.
     * @throws java.lang.UnsupportedOperationException this exception occurs when there is no
     *                                                 managed set of groups to augment. That is,
     *                                                 the current managed set of groups is
     *                                                 <code>null</code>. If the managed set of
     *                                                 groups is <code>null</code>, all groups are
     *                                                 being discovered; thus, requesting that a set
     *                                                 of groups be added to the set of all groups
     *                                                 makes no sense.
     * @throws java.lang.NullPointerException          this exception occurs when either
     *                                                 <code>null</code> is input to the
     *                                                 <code>groups</code> parameter, or one or more
     *                                                 of the elements of the <code>groups</code>
     *                                                 parameter is <code>null</code>. If a
     *                                                 <code>null</code> is input, then the entity
     *                                                 is effectively requesting that "all groups"
     *                                                 be added to the current managed set of
     *                                                 groups; which is not allowed. (Note that if
     *                                                 the entity wishes to change the managed set
     *                                                 of groups from a finite set of names to "all
     *                                                 groups", the <code>setGroups</code> method
     *                                                 should be invoked with <code>null</code>
     *                                                 input.)
     * @see #removeGroups
     */
    public void addGroups(String[] groups) throws IOException;

    /**
     * Replaces all of the group names in the managed set with names from a new set. Once a new
     * group name has been placed in the managed set, if there are lookup services belonging to that
     * group that have already been discovered, no event will be sent to the entity's listener for
     * those particular lookup services. Attempts to discover all (as yet) undiscovered lookup
     * services belonging to that group will continue to be made. <p> If null
     * (<code>ALL_GROUPS</code>) is input to this method, then attempts will be made to discover all
     * (as yet) undiscovered lookup services that are within range, and which are members of any
     * group. If the empty array (<code>NO_GROUPS</code>) is input, then group discovery will cease
     * until this method is invoked with an input parameter that is non-<code>null</code> and
     * non-empty.
     *
     * Note that any entity that invokes this method must have <code>DiscoveryPermission</code> on
     * each of the groups in the new set, otherwise a <code>SecurityException</code> will be
     * propagated through this method.
     *
     * @param groups <code>String</code> array consisting of the group names that will replace the
     *               current names in the managed set.
     * @throws java.io.IOException because an invocation of this method may result in the
     *                             re-initiation of the discovery process, which can throw an
     *                             <code>IOException</code> when socket allocation occurs.
     * @see #getGroups
     */
    public void setGroups(String[] groups) throws IOException;

    /**
     * Deletes a set of group names from the managed set of groups. If the empty array
     * (<code>NO_GROUPS</code>) is input, this method takes no action.
     *
     * @param groups <code>String</code> array consisting of the group names that will be removed
     *               from the managed set.
     * @throws java.lang.UnsupportedOperationException this exception occurs when there is no
     *                                                 managed set of groups from which remove
     *                                                 elements.
     * @throws java.lang.NullPointerException          this exception occurs when <code>null</code>
     *                                                 is input to the <code>groups</code>
     *                                                 parameter.
     * @see #addGroups
     */
    public void removeGroups(String[] groups);
}
