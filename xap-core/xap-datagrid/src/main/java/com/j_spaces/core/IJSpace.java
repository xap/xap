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


package com.j_spaces.core;


import com.gigaspaces.client.IPojoSpace;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.client.SpaceURL;

import net.jini.core.transaction.Transaction;
import net.jini.id.Uuid;

import java.rmi.RemoteException;

/**
 * <b>Notice: Since 7.0 this API is internal and subject to changes in future versions - Use {@link
 * org.openspaces.core.GigaSpace} instead.</b>
 *
 * @author Igor Goldenberg
 * @version 5.1
 * @see org.openspaces.core.GigaSpace
 * @since 1.0
 */
public interface IJSpace extends IPojoSpace {
    /**
     * Wait for no time at all.  This is used as a timeout value in various read and take calls.
     *
     * @see #read
     * @see #readIfExists
     * @see #take
     * @see #takeIfExists
     */
    long NO_WAIT = 0;

    //////////////////////////////
    /// Administration methods ///
    //////////////////////////////

    /**
     * Returns the name of this space.
     */
    String getName();

    String getContainerName();

    /**
     * Returns the {@link SpaceURL} object which was used as the argument for {@link
     * SpaceFinder#find(SpaceURL)} while looking for the space. If a client uses this {@link
     * SpaceURL} when it calls to {@link SpaceFinder#find(SpaceURL)} it should be able to find that
     * space.
     *
     * Notice: The {@link SpaceURL} returned when calling getURL() is different since in that case
     * it returns the {@link SpaceURL} used to initialize the space (a java:// protocol to start an
     * embedded space).
     *
     * @return {@link SpaceURL} object which can be used to find the space proxy while calling
     * {@link SpaceFinder#find(SpaceURL)}
     */
    public SpaceURL getFinderURL();

    /**
     * Returns the {@link SpaceURL} instance which was used to initialize the space.
     */
    public SpaceURL getURL();

    /**
     * Returns the unique {@link Uuid} of this space instance.
     */
    public Uuid getReferentUuid();

    /**
     * Returns true if the space started within a GSC.
     *
     * @deprecated Since 8.0 - This method is reserved for internal usage.
     */
    @Deprecated
    boolean isStartedWithinGSC() throws RemoteException;

    /**
     * Checks whether proxy is connected to embedded or remote space.
     */
    public boolean isEmbedded();

    /**
     * Returns an indication : is this space secured. If for this space defined Security Filter, the
     * space will be secured.
     *
     * @return boolean true if this space secured, otherwise false.
     **/
    public boolean isSecured();

    /**
     * Gets the proxy ReadModifiers.
     */
    public int getReadModifiers();

    /**
     * Sets the read mode modifiers for proxy level. <br>
     */
    public int setReadModifiers(int readModifiers);

    /**
     * Gets the proxyUpdateModifiers.
     */
    public int getUpdateModifiers();

    /**
     * Sets the update mode modifiers for proxy level.
     */
    public int setUpdateModifiers(int newModifiers);

    /**
     * Returns status of Optimistic Lock protocol.
     */
    public boolean isOptimisticLockingEnabled();

    /**
     * Enable/Disable Optimistic Lock protocol.
     */
    public void setOptimisticLocking(boolean enabled);

    /**
     * Returns <code>true</code> if this proxy FIFO enabled, otherwise <code>false</code>.
     */
    @Deprecated
    public boolean isFifo();

    /**
     * Sets FIFO mode for proxy.
     */
    @Deprecated
    public void setFifo(boolean enabled);

    /**
     * Returns the admin object to the remote part of this space.
     * <pre><code>
     * // get a list of classes and the number of entries of each class
     * {@link com.j_spaces.core.admin.IRemoteJSpaceAdmin} remoteAdmin =
     * (IRemoteJSpaceAdmin)serverAdmin.getAdmin();
     * Object classList[] = remoteAdmin.getRuntimeInfo().m_ClassNames.toArray();
     * List numOFEntries = remoteAdmin.getRuntimeInfo().m_NumOFEntries;
     * </code></pre>
     *
     * @return the remoteAdmin object to the remote part of this space.
     * @throws RemoteException if a communication error occurs
     */
    Object getAdmin() throws java.rmi.RemoteException;

    /**
     * returns itself in case of a direct proxy or the real proxy in case of a local cache or a
     * view.
     *
     * @return IDirectSpaceProxy
     */
    IDirectSpaceProxy getDirectProxy();

    /**
     * Checks whether the space is alive and accessible.
     *
     * @throws java.rmi.RemoteException when space was unreachable
     */
    void ping() throws java.rmi.RemoteException;

    /**
     * Drops all Class's entries and all its templates from the space. Calling this method will
     * remove all internal meta data related to this class stored in the space. When using
     * persistent spaced the relevant RDBMS table will be dropped. It is the caller responsibility
     * to ensure that no entries from this class are written to the space while this method is
     * called.
     *
     * <pre><code>
     * Message message = new Message("Hello");
     * Lease lease = space.write(message, null, Lease.FOREVER);
     *
     * serverAdmin.dropClass( Message.class.getName());
     * </code></pre>
     *
     * @param className name of class to delete.
     * @throws DropClassException Failed to drop desired class.
     * @throws RemoteException    if a communication error occurs
     * @see com.gigaspaces.client.IPojoSpace#clear(Object, Transaction)
     **/
    public void dropClass(String className) throws java.rmi.RemoteException, DropClassException;

    public String getCacheTypeName();
}
