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
package com.sun.jini.mahalo;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.stubcache.StubId;

import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.core.transaction.server.TransactionParticipant;
import net.jini.security.ProxyPreparer;

import java.io.Serializable;
import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Sun Microsystems, Inc.
 */

class ParticipantHandle implements Serializable, TransactionConstants {
    static final long serialVersionUID = -1776073824495304317L;

    /**
     * Cached reference to prepared participant.
     */
    private transient TransactionParticipant _part;

    /**
     * @serial
     */
    private StorableObject storedpart;

    /**
     * @serial
     */
    private long crashcount = 0;

    /**
     * @serial
     */
    private int prepstate;

    private StubId _stubId;

    private boolean _disableDisjoin;

    //the following fields used for fail-over of dist-xtn
    private int _partitionId;  //-1 if N.A.
    private String _clusterName; //null if N.A.
    private IDirectSpaceProxy _clusterProxy;//null if N.A.
    private volatile boolean _prepared;     //is this par prepared ????

    private volatile CannotCommitException _commitEx;
    private volatile CannotAbortException _abortEx;
    /**
     * Logger for persistence related messages
     */
    private static final Logger persistenceLogger =
            TxnManagerImpl.persistenceLogger;


    /**
     * Create a new node that is equivalent to that node
     */
    ParticipantHandle(TransactionParticipant preparedPart,
                      long crashcount, StubId stubId, boolean persistent)
            throws RemoteException {
        this(preparedPart,
                crashcount, stubId, persistent, -1, null, null);
    }

    ParticipantHandle(TransactionParticipant preparedPart,
                      long crashcount, StubId stubId, boolean persistent, int partitionId, String clusterName, IDirectSpaceProxy clusterProxy)
            throws RemoteException {
        if (preparedPart == null)
            throw new NullPointerException(
                    "TransactionParticipant argument cannot be null");
        _partitionId = partitionId;
        _clusterName = clusterName;
        try {
            _stubId = stubId;
            if (persistent)
                storedpart = new StorableObject(preparedPart);
            this._part = preparedPart;
            this.crashcount = crashcount;
            if (crashcount != ServerTransaction.EMBEDDED_CRASH_COUNT)
                _disableDisjoin = true;

            if (clusterProxy != null)
                _clusterProxy = clusterProxy;

        } catch (RemoteException re) {
            if (persistenceLogger.isWarnEnabled()) {
                persistenceLogger.warn(
                        "Cannot store the TransactionParticipant", re);
            }
        }
        this.prepstate = ACTIVE;
    }

    long getCrashCount() {
        return crashcount;
    }

    TransactionParticipant getParticipant() {
        return _part;
    }

    // Only called by service initialization code 
    void restoreTransientState(ProxyPreparer recoveredListenerPreparer)
            throws RemoteException {
        if (recoveredListenerPreparer == null)
            throw new NullPointerException(
                    "Preparer argument cannot be null");
    /*
     * ProxyPreparation potentially make remote calls. So,
	 * need to make sure that locks aren't being held across this 
	 * invocation.
	 */
        _part = (TransactionParticipant)
                recoveredListenerPreparer.prepareProxy(storedpart.get());
    }

    StorableObject getStoredPart() {
        return storedpart;
    }

    synchronized void setPrepState(int state) {
        switch (state) {
            case PREPARED:
            case NOTCHANGED:
            case COMMITTED:
            case ABORTED:
                break;
            default:
                throw new IllegalArgumentException("ParticipantHandle: " +
                        "setPrepState: cannot set to " +
                        com.sun.jini.constants.TxnConstants.getName(state));
        }

        this.prepstate = state;
    }

    synchronized int getPrepState() {
        return prepstate;
    }


    boolean compareTo(ParticipantHandle other) {
        if (storedpart != null)
            return storedpart.equals(other);
        else if (other == null)
            return false;

        return false;
    }

    public StubId getStubId() {
        return _stubId;
    }


    public void setStubId(StubId stubid) {
        _stubId = stubid;
    }

    public boolean isDisableDisjoin() {
        return _disableDisjoin;
    }

    public void setDisableDisjoin() {
        if (!_disableDisjoin)
            _disableDisjoin = true;
    }

    public IDirectSpaceProxy getClusterProxy() {
        return _clusterProxy;//null if N.A.
    }

    public void setClusterProxy(IDirectSpaceProxy clusterProxy) {
        _clusterProxy = clusterProxy;
    }

    public String getClusterName() {
        return _clusterName;
    }

    public void setClusterName(String cname) {
        _clusterName = cname;
    }

    public void setPartitionId(int pid) {
        _partitionId = pid;
    }

    public int getPartionId() {
        return _partitionId;
    }

    public boolean isSuitableForCommitFailover() {
        return isSuitableForFailover() && _prepared;
    }

    public boolean isNeedProxyInCommit() {
        return (_partitionId >= 0 && _clusterProxy == null && _clusterName != null);
    }

    public boolean isSuitableForFailover() {
        return (_partitionId >= 0 && _clusterProxy != null && _clusterName != null);
    }

    public boolean isPrepared() {
        return _prepared;
    }

    public void setPrepared() {
        _prepared = true;
    }

    public CannotCommitException getCommitException() {
        return _commitEx;
    }

    public CannotAbortException getAbortException() {
        return _abortEx;
    }

    public void setCommitException(CannotCommitException ex) {
        _commitEx = ex;
    }

    public void setAbortException(CannotAbortException ex) {
        _abortEx = ex;
    }

    /**
     * Return the <code>hashCode</code> of the embedded <code>TransactionParticipant</code>.
     */
    @Override
    public int hashCode() {
        if (_stubId != null)
            return _stubId.hashCode();
        return _part.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that)
            return true;

        ParticipantHandle h = (ParticipantHandle) that;
        if (_stubId != null) {
            if (_stubId == h._stubId)
                return true;
            if (h._stubId != null)
                return _stubId.equals(h._stubId);
        }
        return _part.equals(h._part);
    }
}
