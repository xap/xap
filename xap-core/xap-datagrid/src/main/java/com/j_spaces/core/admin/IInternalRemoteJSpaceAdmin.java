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

import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.grid.zone.GridZoneProvider;
import com.gigaspaces.internal.jvm.JVMInfoProvider;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.os.OSInfoProvider;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.lrmi.nio.info.NIOInfoProvider;
import com.gigaspaces.management.space.LocalCacheDetails;
import com.gigaspaces.management.space.LocalViewDetails;
import com.gigaspaces.management.transport.ITransportConnection;
import com.gigaspaces.server.space.suspend.SuspendType;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceCopyStatus;
import com.j_spaces.core.client.TransactionInfo;
import com.j_spaces.core.client.UnderTxnLockedObject;
import com.j_spaces.core.exception.SpaceAlreadyStartedException;
import com.j_spaces.core.exception.SpaceAlreadyStoppedException;

import net.jini.core.transaction.Transaction;

import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * Extends IRemoteJSpaceAdmin to include internal methods.
 *
 * @author Guy Korland
 */
public interface IInternalRemoteJSpaceAdmin extends IRemoteJSpaceAdmin, NIOInfoProvider, OSInfoProvider, JVMInfoProvider, GridZoneProvider {
    /**
     * disable remote.
     *
     * @throws RemoteException if a communication error occurs
     **/
    public void disableStub() throws RemoteException;


    /**
     * Returns a byte representation of the cluster configuration file.
     *
     * @return byte representation of the cluster configuration file
     * @throws RemoteException if a communication error occurs
     */
    public byte[] getClusterConfigFile() throws RemoteException;

    /**
     * set the cluster configuration according to the given byte representation of the configuration
     * file.
     *
     * @param config byte representation of the configuration file
     * @throws RemoteException if a communication error occurs
     */
    public void setClusterConfigFile(byte[] config) throws RemoteException;

    RuntimeHolder getRuntimeHolder() throws RemoteException;

    boolean isStartedWithinGSC() throws RemoteException;

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
     * @param template               EntryPacket template or null.
     * @param includeNotifyTemplates - if true ALL notify templates will be copied too
     * @param chunkSize              Chunk size (batch) for this copy operation.
     * @return SpaceCopyStatus Contains the status information of spaceCopy operation.
     * @throws RemoteException if a communication error occurs
     **/
    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template, boolean includeNotifyTemplates, int chunkSize)
            throws RemoteException;

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
     * @param template               EntryPacket template or null.
     * @param includeNotifyTemplates - if true ALL notify templates will be copied too
     * @param chunkSize              Chunk size (batch) for this copy operation.
     * @param sc                     SpaceContext for security check in the server side.
     * @return SpaceCopyStatus Contains the status information of spaceCopy operation.
     * @throws RemoteException if a communication error occurs
     **/
    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template, boolean includeNotifyTemplates, int chunkSize, SpaceContext sc)
            throws RemoteException;

    /**
     * Given ISpaceInternal of a remote space, Copy all entries/notify templates from the space.
     *
     * <pre><code>
     * <b>Usage:</b>
     *   SpaceCopyStatus copyStatus = spaceAdmin.spaceCopy( remoteProxy, remoteURL, remoteName,
     * template, false 1000, sc);
     *   System.out.println("Copy-Status: " + copyStatus );
     * </code></pre>
     *
     * @param remoteProxy            ISpaceInternal, a proxy to the remote space to copy from
     * @param remoteSpaceUrl         the remote space url.
     * @param remoteSpaceName        the remote space name.
     * @param template               A template for identified entries that should be copied.
     * @param includeNotifyTemplates - if true ALL notify templates will be copied too
     * @param chunkSize              Chunk size (batch) for this copy operation.
     * @param spaceContext           SpaceContext for security check in the server side.
     * @param remoteSpaceContext     remoteSpaceContext for security check in the server side.
     * @return SpaceCopyStatus Contains the status information of spaceCopy operation.
     * @throws RemoteException if a communication error occurs
     **/
    public SpaceCopyStatus spaceCopy(IRemoteSpace remoteProxy, String remoteSpaceUrl, String remoteSpaceName, ITemplatePacket template, boolean includeNotifyTemplates, int chunkSize, SpaceContext spaceContext, SpaceContext remoteSpaceContext)
            throws RemoteException;

    /**
     * Returns all the transactions with the specific parameters.
     *
     * @param type   TransactionInfo.Types.LOCAL, TransactionInfo.Types.JINI or
     *               TransactionInfo.Types.XA. Use TransactionInfo.Types.All for all types;
     * @param status can be one of the status defined in <code>TransactionConstants</code>
     * @return all the TransactionInfos of the transactions that matches the given parameters
     * @throws RemoteException if a communication error occurs
     */
    public TransactionInfo[] getTransactionsInfo(int type, int status) throws RemoteException;

    public int countTransactions(int type, int status) throws RemoteException;


    /**
     * Returns a list of pending templates.
     *
     * @param className name of the class to search
     * @return list of pending template for the given class
     */
    public List<TemplateInfo> getTemplatesInfo(String className) throws RemoteException;

    /**
     * @return returns all the transport connection information for this Space.
     * @deprecated Since 10.1 - Use countIncomingConnections instead
     */
    @Deprecated
    public List<ITransportConnection> getConnectionsInfo() throws RemoteException;

    /**
     * @return returns all the transport connection information for this Space.
     */
    public int countIncomingConnections() throws RemoteException;


    /**
     * getLockedObjects is called by the GUI. it returns Info about all the locked Object by the
     * given Transaction.
     *
     * @param txn - the Transaction that locks the Objects.
     * @return List of UnderTxnLockedObject
     */
    public List<UnderTxnLockedObject> getLockedObjects(Transaction txn) throws RemoteException;

    /**
     * Get the current {@link SpaceMode} which represents the current state of this participant in
     * the election process. <p> In case of a non-clustered participant, where no election process
     * takes place, the return value is always {@link SpaceMode#PRIMARY}. <p>
     *
     * @return the current {@link SpaceMode} of the space.
     * @throws RemoteException if an exception occurred executing this remote method.
     * @since 6.6
     */
    public SpaceMode getSpaceMode() throws RemoteException;

    /**
     * Adds a listener to be called back with {@link ISpaceModeListener} mode changes. The change (
     * {@linkplain SpaceMode SpaceMode} ) represents the result of an election process of this
     * participant space. The returned value represents the current state of this participant in the
     * election process. <p> In case of a non-clustered participant, the return value is always
     * {@link SpaceMode#PRIMARY}. Hence, no election process will take place and the listener will
     * not be called.
     *
     * @param listener a listener to be called back on space mode changes.
     * @return the current {@link SpaceMode} of the space.
     * @throws RemoteException if an exception occurred executing this remote method.
     * @see ISpaceModeListener
     * @see SpaceMode
     * @since 6.0
     */
    public SpaceMode addSpaceModeListener(ISpaceModeListener listener) throws RemoteException;

    /**
     * Removes this listener so that it no longer receives call backs of space mode changes.
     * Invocation has no additional effect if the listener was already removed or was not existent
     * in the first place.
     *
     * @param listener a listener to be removed.
     * @throws RemoteException if an exception occurred executing this remote method.
     * @see #addSpaceModeListener(ISpaceModeListener)
     * @since 6.0
     */
    public void removeSpaceModeListener(ISpaceModeListener listener) throws RemoteException;

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
     */
    public ITypeDesc getClassDescriptor(String className) throws RemoteException;

    /**
     * Forcefully make this space to be primary
     */
    public void forceMoveToPrimary() throws RemoteException;

    public String getReplicationDump() throws RemoteException;

    /**
     * @since 9.5.0
     */
    public Map<String, LocalCacheDetails> getLocalCacheDetails() throws RemoteException;

    /**
     * @since 9.5.0
     */
    public Map<String, LocalViewDetails> getLocalViewDetails() throws RemoteException;

    /**
     * Attempt to start this space.
     *
     * <pre><code>
     * <b>Usage:</b>
     *  spaceAdmin.start();
     * </code></pre>
     *
     * @throws RemoteException              - Failed to start space.
     * @throws SpaceAlreadyStartedException - The space is already started
     * @deprecated To stop and start a space, restart the corresponding processing unit instance
     */
    @Deprecated
    public void start() throws RemoteException, SpaceAlreadyStartedException;

    /**
     * Attempt to stop this space.
     *
     * <pre><code>
     * <b>Usage:</b>
     *  spaceAdmin.stop();
     * </code></pre>
     *
     * @throws RemoteException              - Failed to stop space.
     * @throws SpaceAlreadyStoppedException - The space is already stopped
     * @deprecated To stop and start a space, restart the corresponding processing unit instance
     */
    @Deprecated
    public void stop() throws RemoteException, SpaceAlreadyStoppedException;

    /**
     * Restarts the space.
     *
     * <pre><code>
     * <b>Usage:</b>
     *   try
     *   {
     *  spaceAdmin.restart();
     *   }
     *   catch( java.rmi.RemoteException ex )
     *   {
     *  System.err.println("Failed to restart space: " + ((IJSpace)space).getName());
     *  System.err.println("Exception: " + ex);
     *   }
     * </code></pre>
     *
     * @throws RemoteException Failed to restart space.
     * @deprecated To restart a space, restart the corresponding processing unit instance
     **/
    @Deprecated
    void restart() throws RemoteException;

    /**
     * Adds a listener to be called back with {@link SuspendTypeChangedInternalListener} suspend type changes.
     * @param listener - {@link SuspendTypeChangedInternalListener} that will receive notification when a space suspend type changes ({@link SuspendType})
     */
    SuspendType addSpaceSuspendTypeListener(SuspendTypeChangedInternalListener listener);

    /**
     * Removes this listener so that it no longer receives call backs of space suspend type changes.
     * @param listener - {@link SuspendTypeChangedInternalListener} that will stop receiving notification when a space suspend type changes ({@link SuspendType})
     */
    void removeSpaceSuspendTypeListener(SuspendTypeChangedInternalListener listener);
}