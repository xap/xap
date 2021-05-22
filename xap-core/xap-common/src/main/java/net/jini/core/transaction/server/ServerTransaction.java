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
package net.jini.core.transaction.server;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.serialization.SmartExternalizable;
import com.sun.jini.mahalo.TxnMgrProxy;

import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.CannotJoinException;
import net.jini.core.transaction.ManagedTransaction;
import net.jini.core.transaction.TimeoutExpiredException;
import net.jini.core.transaction.UnknownTransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * Class implementing the <code>Transaction</code> interface, for use with transaction participants
 * that implement the default transaction semantics.
 *
 * @author Sun Microsystems, Inc.
 * @see net.jini.core.transaction.Transaction
 * @see NestableServerTransaction
 * @see TransactionManager
 * @see net.jini.core.transaction.TransactionFactory
 * @since 1.0
 */
@com.gigaspaces.api.InternalApi
public class  ServerTransaction implements ManagedTransaction, java.io.Serializable, SmartExternalizable, ISwapExternalizable {
    private static final long serialVersionUID = 4552277137549765374L;

    // NOTE: All were final fields (can't be now since we implement externalizable)
    /**
     * The transaction manager.
     *
     * @serial
     */
    public ExtendedTransactionManager mgr;
    /**
     * The transaction id.
     *
     * @serial
     */
    public long id;

    private long lease;

    private TransactionParticipantDataImpl metaData;

    //true if is an embedded proxy-side coordinator. this indicator is passed
    // to remote spaces, but tured off when xtn is passed to another proxy 
    //and from that proxy to a participant 
    private boolean _embeddedMgrInProxy;


    //true only if this instance of txn is inside the proxy when created
    //by an embedded manager
    private transient boolean _embeddedMgrProxySideInstance;

    //crash count for embedded mahalo in proxy
    public static final long EMBEDDED_CRASH_COUNT = Long.MAX_VALUE;
    private static final long DEFAULT_COMMIT_ABORT_TIMEOUT = Long.MAX_VALUE;

    /**
     * Do not use, required for externalizable
     */
    public ServerTransaction() {
    }

    /**
     * Simple constructor.  Clients should not call this directly, but should instead use
     * <code>TransactionFactory</code>.
     *
     * @param mgr   the manager for this transaction
     * @param id    the transaction id
     * @param lease the lease of the transaction.
     */
    public ServerTransaction(TransactionManager mgr, long id, long lease) {
        this.mgr = (ExtendedTransactionManager) mgr;
        this.id = id;
        this.lease = lease;
    }

    public ServerTransaction(TransactionManager mgr, long id) {
        this(mgr, id, -1);
    }

    public ServerTransaction(TransactionManager mgr, long id, TransactionParticipantDataImpl metaData) {
        this(mgr, id);
        this.metaData = metaData;
    }

    public static ServerTransaction create(TransactionManager mgr, long id, long lease)
            throws RemoteException {
        ServerTransaction txn = new ServerTransaction(mgr, id, lease);
        txn.initEmbedded();
        return txn;
    }

    protected void initEmbedded() throws RemoteException {
        if (mgr.needParticipantsJoin()) {
            //check for embedded manager
            if (((TxnMgrProxy) mgr).isEmbeddedMgr()) {
                setEmbeddedMgrInProxy(true);
                setEmbeddedMgrProxySideInstance(true);
            }
        } else//local mng
        {
            setEmbeddedMgrInProxy(true);
            setEmbeddedMgrProxySideInstance(true);
        }
    }

    public long getLease() {
        return lease;
    }

    public void setLease(long lease) {
        this.lease = lease;
    }

    /**
     * Two instances are equal if they have the same transaction manager and the same transaction
     * id.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (!(other instanceof ServerTransaction))
            return false;

        ServerTransaction t = (ServerTransaction) other;
        return (id == t.id && mgr.equals(t.mgr));
    }

    @Override
    public int hashCode() {
        return (int) id ^ mgr.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [id=" + id + ", manager=" + mgr + "]";
    }

    @Override
    public void commit()
            throws UnknownTransactionException, CannotCommitException, RemoteException {
        try {
            mgr.commit(id, DEFAULT_COMMIT_ABORT_TIMEOUT);
        } catch (TimeoutExpiredException e) {
            // TOLOG LB:
        }
    }

    @Override
    public void commit(long waitFor)
            throws UnknownTransactionException, CannotCommitException, TimeoutExpiredException, RemoteException {
        mgr.commit(id, waitFor);
    }

    @Override
    public void abort()
            throws UnknownTransactionException, CannotAbortException, RemoteException {
        try {
            mgr.abort(id, DEFAULT_COMMIT_ABORT_TIMEOUT);
        } catch (TimeoutExpiredException e) {
            // TOLOG LB:
        }
    }

    @Override
    public void abort(long waitFor)
            throws UnknownTransactionException, CannotAbortException, TimeoutExpiredException, RemoteException {
        mgr.abort(id, waitFor);
    }

    /**
     * Join the transaction. The <code>crashCount</code> marks the state of the storage used by the
     * participant for transactions. If the participant attempts to join a transaction more than
     * once, the crash counts must be the same. Each system crash or other event that destroys the
     * state of the participant's unprepared transaction storage must cause the crash count to
     * increase by at least one.
     *
     * @param part       the participant joining the transaction
     * @param crashCount the participant's current crash count
     * @throws UnknownTransactionException if the transaction is unknown to the transaction manager,
     *                                     either because the transaction ID is incorrect or because
     *                                     the transaction is complete and its state has been
     *                                     discarded by the manager.
     * @throws CannotJoinException         if the transaction is known to the manager but is no
     *                                     longer active.
     * @throws CrashCountException         if the crash count provided for the participant differs
     *                                     from the crash count in a previous invocation of the same
     *                                     pairing of participant and transaction
     * @throws RemoteException             if there is a communication error
     */
    public void join(TransactionParticipant part, long crashCount)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (crashCount == EMBEDDED_CRASH_COUNT)
            // used only in embedded mahalo- pass the user ServerTrasaction- allows
            // * updaing the lease interval in it
            mgr.join(id, part, crashCount, this);
        else
            mgr.join(id, part, crashCount);
    }

    protected void join(TransactionParticipant part, long crashCount,
                        int partitionId, String clusterName, Object proxy)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (crashCount == EMBEDDED_CRASH_COUNT)
            // used only in embedded mahalo- pass the user ServerTrasaction- allows
            // * updaing the lease interval in it 
            mgr.join(id,
                    part,
                    crashCount,
                    this,
                    partitionId,
                    clusterName,
                    proxy);
        else
            throw new UnsupportedOperationException(" supported only for embedded join");
    }

    public void join(TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (crashCount == EMBEDDED_CRASH_COUNT)
            throw new UnsupportedOperationException(" not supported for embedded join");

        mgr.join(id, part, crashCount, partitionId, clusterName);
    }

    /**
     * Returns the current state of the transaction.  The returned state can be any of the
     * <code>TransactionConstants</code> values.
     *
     * @return an <code>int</code> representing the state of the transaction
     * @throws UnknownTransactionException if the transaction is unknown to the transaction manager,
     *                                     either because the transaction ID is incorrect or because
     *                                     the transaction is complete and its state has been
     *                                     discarded by the manager.
     * @throws RemoteException             if there is a communication error
     * @see TransactionConstants
     */
    public int getState() throws UnknownTransactionException, RemoteException {
        return mgr.getState(id);
    }

    /**
     * Return true if the transaction has a parent, false if the transaction is top level.
     *
     * @return true if the transaction has a parent, false if the transaction is top level.
     */
    public boolean isNested() {
        return false;
    }

    /**
     * @return the metaData
     */
    public TransactionParticipantDataImpl getMetaData() {
        return metaData;
    }

    /**
     * @param metaData the metaData to set
     */
    public void setMetaData(TransactionParticipantDataImpl metaData) {
        this.metaData = metaData;
    }

    @Override
    public boolean isEmbeddedMgrInProxy() {
        return _embeddedMgrInProxy;
    }

    private void setEmbeddedMgrInProxy(boolean value) {
        _embeddedMgrInProxy = value;
    }

    public boolean isEmbeddedMgrProxySideInstance() {
        return _embeddedMgrProxySideInstance;
    }

    public void setEmbeddedMgrProxySideInstance(boolean value) {
        _embeddedMgrProxySideInstance = value;
    }

    /**
     * returns true if this  the txn participants need to join it in contrary to a xtn which the
     * participants are known prior to txn propagation
     *
     * @return true if its a  the xtn mgr  requires the txn participants to join
     */
    @Override
    public boolean needParticipantsJoin()
            throws RemoteException {
        return mgr.needParticipantsJoin();
    }

    public boolean isXid() {
        return false;
    }

    /**
     * Creates an instance of this class for cloning using the {@link ServerTransaction#createCopy()}
     * method.
     */
    protected ServerTransaction createInstance() {
        return new ServerTransaction();
    }

    public ServerTransaction createCopy() {
        ServerTransaction other = createInstance();
        other.mgr = mgr;
        other.id = id;
        other.lease = lease;
        other._embeddedMgrInProxy = _embeddedMgrInProxy;
        other._embeddedMgrProxySideInstance = _embeddedMgrProxySideInstance;
        other.metaData = metaData;
        return other;
    }

    @Deprecated
    public void joinIfNeededAndEmbedded(TransactionParticipant participant)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        if (isEmbeddedMgrProxySideInstance() && needParticipantsJoin())
            join(participant, ServerTransaction.EMBEDDED_CRASH_COUNT /* crashcount */);
    }

    public boolean joinIfNeededAndEmbedded(TransactionParticipant participant, int partitionId, String clusterName, Object proxy)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        if (isEmbeddedMgrProxySideInstance() && needParticipantsJoin()) {
            if (partitionId < 0)
                join(participant, ServerTransaction.EMBEDDED_CRASH_COUNT /* crashcount */);
            else
                join(participant, ServerTransaction.EMBEDDED_CRASH_COUNT /* crashcount */, partitionId, clusterName, proxy);

            return true;
        }
        return false;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        serialize(out);
    }

    private void serialize(ObjectOutput out)
            throws IOException {
        boolean lightMgr = _embeddedMgrProxySideInstance && needParticipantsJoin() && mgr instanceof TxnMgrProxy;
        if (lightMgr) {
            TxnMgrProxy cur = (TxnMgrProxy) mgr;
            out.writeObject(cur.createLightProxy());
        } else
            out.writeObject(mgr);

        out.writeLong(id);
        out.writeLong(lease);
        //note: written _embeddedMgrProxySideInstance will be read into _embeddedMgr
        out.writeBoolean(_embeddedMgrProxySideInstance);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    private final void deserialize(ObjectInput in)
            throws ClassNotFoundException, IOException {
        mgr = (ExtendedTransactionManager) in.readObject();
        id = in.readLong();
        lease = in.readLong();
        //note: written _embeddedMgrProxySideInstance will be read into _embeddedMgr
        _embeddedMgrInProxy = in.readBoolean();
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    public ExtendedTransactionManager getTransactionManager() {
        return mgr;
    }
}
