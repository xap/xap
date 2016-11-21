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


package com.j_spaces.core.admin;

import com.j_spaces.core.DropClassException;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceCopyStatus;
import com.j_spaces.core.client.BasicTypeInfo;
import com.j_spaces.core.cluster.ClusterPolicy;
import org.jini.rio.boot.SpaceInstanceRemoteClassLoaderInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This interface contains all the administrative methods that GigaSpaces provides to control the
 * JavaSpaces service.
 *
 * <pre><code>
 * <b>Usage:</b>
 * 	IJSpace space = (IJSpace) SpaceFinder.find(spaceUrl);
 * 	IRemoteJSpaceAdmin spaceAdmin = (IRemoteJSpaceAdmin)space.getAdmin();
 *
 * The code examples on each method assumes the above usage.
 * </code></pre>
 *
 * @author Igor Goldenberg.
 */
public interface IRemoteJSpaceAdmin extends Remote {
    /* replication status values as viewed by the administrator using the getReplicationStatus() API */
    /**
     * The replication is active.  @see getReplicationStatus()
     *
     * @deprecated Since 8.0. This constant is relevant only in the old depracated replication
     * module and will be removed in a future version.
     */
    @Deprecated
    public final static int REPLICATION_STATUS_ACTIVE = 0;

    /**
     * The replication is disconnected. @see #getReplicationStatus()
     *
     * @deprecated Since 8.0. This constant is relevant only in the old depracated replication
     * module and will be removed in a future version.
     */
    @Deprecated
    public final static int REPLICATION_STATUS_DISCONNECTED = 1;

    /**
     * The replication is disabled. @see #getReplicationStatus()
     *
     * @deprecated Since 8.0. This constant is relevant only in the old depracated replication
     * module and will be removed in a future version.
     */
    @Deprecated
    public final static int REPLICATION_STATUS_DISABLED = 2;

    /**
     * Returns the Space Name.
     *
     * <pre><code>
     * <b>Usage:</b>
     * 	spaceAdmin.getName()
     * </code></pre>
     *
     * @return the space's name
     * @throws RemoteException Failed to get name
     **/
    String getName() throws RemoteException;

    /**
     * Returns <code>SpaceRuntimeInfo</code>.
     *
     * <pre><code>
     * <b>Usage:</b>
     *   SpaceRuntimeInfo runtime = spaceAdmin.getRuntimeInfo();
     *   List classNames = runtime.m_ClassNames;
     *   List numOfEntries = runtime.m_NumOFEntries;
     * </code></pre>
     *
     * @return SpaceRuntimeInfo which holds the classes and the number of entries currently in the
     * space.
     * @throws RemoteException if a communication error occurs
     **/
    SpaceRuntimeInfo getRuntimeInfo() throws RemoteException;

    /**
     * Returns <code>SpaceRuntimeInfo</code> for specific class name and its subclasses.
     *
     * <pre><code>
     * <b>Usage:</b>
     * 	 String className = ExampleObject.class.getName();
     *   SpaceRuntimeInfo runtime = spaceAdmin.getRuntimeInfo(className);
     *   List classNames = runtime.m_ClassNames;
     *   List numOfEntries = runtime.m_NumOFEntries;
     * </code></pre>
     *
     * @return SpaceRuntimeInfo which holds the classes and the number of entries of given class and
     * its subclasses currently in space.
     * @throws RemoteException if a communication error occurs
     **/
    SpaceRuntimeInfo getRuntimeInfo(String className) throws RemoteException;

    /**
     * Returns <code>SpaceConfig</code> of this space.
     *
     * <pre><code>
     * <b>Usage:</b>
     * 	SpaceConfig config = spaceAdmin.getConfig();
     * 	if (config != null)
     * 		boolean isClustered = config.m_isClustered;
     * </code></pre>
     *
     * @return SpaceConfig - Contains all information about the space configuration.
     * @throws RemoteException if a communication error occurs
     **/
    SpaceConfig getConfig() throws RemoteException;

    /**
     * Ping to space to see if alive. If successful, this does not indicate that the space is in a
     * running state.
     *
     * <pre><code><b>Usage:</b>
     * try
     * {
     *     spaceAdmin.ping();
     * }
     * catch (RemoteException re)
     * {
     *     System.err.println("Space not accessible");
     * }</code></pre>
     *
     * @throws RemoteException if a communication error occurs
     * @see #getState()
     **/
    void ping() throws RemoteException;

    /**
     * Given Url of a remote space, Copy all entries/notify templates from the space.
     *
     * <pre><code>
     * <b>Usage:</b>
     *   SpaceCopyStatus copyStatus = spaceAdmin.spaceCopy( sourceSpaceURL, template, false );
     *   System.out.println("Copy-Status: " + copyStatus );
     * </code></pre>
     *
     * @param remoteUrl              url of remote space to copy from
     * @param template               Entry template or null.
     * @param includeNotifyTemplates - if true ALL notify templates will be copied too
     * @param chunkSize              Chunk size (batch) for this copy operation.
     * @return SpaceCopyStatus Contains the status information of spaceCopy operation.
     * @throws RemoteException if a communication error occurs
     */
    public SpaceCopyStatus spaceCopy(String remoteUrl, Object template, boolean includeNotifyTemplates, int chunkSize)
            throws RemoteException;

    public SpaceCopyStatus spaceCopy(IJSpace remoteSpace, Object template, boolean includeNotifyTemplates, int chunkSize) throws RemoteException;

    /**
     * returns the replication member-status relation of all the replication groups.
     *
     * <pre><code>
     * <b>Usage:</b>
     *   Object[] replStatus = spaceAdmin.getReplicationStatus();
     *   String[] peers = (String[])replStatus[0];
     *   int[] status = (int[])replStatus[1];
     * </code></pre>
     *
     * @return an array of remote member names array and replication status array. Replication
     * status can have one of the following values: IRemoteJSpaceAdmin.REPLICATION_STATUS_ACTIVE
     * IRemoteJSpaceAdmin.REPLICATION_STATUS_DISCONNECTED IRemoteJSpaceAdmin.REPLICATION_STATUS_DISABLED
     * @throws RemoteException if a communication error occurs
     * @deprecated Since 8.0 - This method is relevant only in the old deprecated replication module
     * and will be removed in a future version.
     */
    @Deprecated
    public Object[] getReplicationStatus() throws RemoteException;

    /**
     * Get the basic class information from the space directory.
     *
     * <pre><code>
     * <b>Usage:</b>
     *   com.j_spaces.core.client.BasicTypeInfo classInfo
     * 	= spaceAdmin.getClassTypeInfo( className );
     *
     *   String[] fieldNames = classInfo.m_FieldsNames;
     *   String[] fieldTypes = classInfo.m_FieldsTypes;
     * </code></pre>
     *
     * @param className - The name of the class to retrieve the information from.
     * @return BasicTypeInfo - Which provides class information.
     * @throws RemoteException if a communication error occurs
     * @deprecated Since 8.0 - Use <code>GigaSpaceTypeManager.getTypeDescriptor</code> instead.
     */
    @Deprecated
    public BasicTypeInfo getClassTypeInfo(String className) throws RemoteException;

    /**
     * Return the present ClusterPolicy objects.
     *
     * @return a ClusterPolicy that represents current cluster
     * @throws RemoteException if a communication error occurs
     */
    public ClusterPolicy getClusterPolicy() throws RemoteException;

    /**
     * Drop all Class entries and all its templates from the space. Calling this method will remove
     * all internal meta data related to this class stored in the space. When using persistent
     * spaced the relevant RDBMS table will be dropped. It is the caller responsibility to ensure
     * that no entries from this class are written to the space while this method is called. This
     * method is protected through the space Default Security Filter. Admin permissions required to
     * execute this request successfully.
     *
     * <pre><code>
     * <b>Usage:</b>
     * 	spaceAdmin.dropClass( className );
     * </code></pre>
     *
     * @param className name of class to delete.
     * @throws DropClassException Failed to drop desired class.
     * @throws RemoteException    if a communication error occurs
     **/
    public void dropClass(String className, SpaceContext sc) throws RemoteException, DropClassException;

    /**
     * Returns the particular condition of this space.
     *
     * @return state - ISpaceState.STOPPED, ISpaceState.STARTED and etc.
     * @throws RemoteException if a communication error occurs
     * @see com.j_spaces.core.ISpaceState
     */
    public int getState() throws RemoteException;

    /**
     * Return the info about the space classloader
     *
     * @return a SpaceInstanceRemoteClassLoaderInfo that represents the space classloader's
     * @throws RemoteException if a communication error occurs
     */
    SpaceInstanceRemoteClassLoaderInfo getSpaceInstanceRemoteClassLoaderInfo() throws RemoteException;
}
