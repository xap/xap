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

import com.gigaspaces.serialization.SmartExternalizable;
import com.sun.jini.landlord.LandlordLease;
import com.sun.jini.proxy.DefaultProxyPivot;
import com.sun.jini.proxy.MarshalPivot;
import com.sun.jini.proxy.MarshalPivotProvider;

import net.jini.admin.Administrable;
import net.jini.core.constraint.MethodConstraints;
import net.jini.core.constraint.RemoteMethodControl;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.CannotJoinException;
import net.jini.core.transaction.TimeoutExpiredException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.CrashCountException;
import net.jini.core.transaction.server.ExtendedTransactionManager;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionParticipant;
import net.jini.id.ReferentUuid;
import net.jini.id.Uuid;
import net.jini.security.proxytrust.ProxyTrustIterator;
import net.jini.security.proxytrust.SingletonProxyTrustIterator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.rmi.RemoteException;
import java.util.List;

/**
 * A <code>TxnMgrProxy</code> is a proxy for the transaction manager service. This is the object
 * passed to clients of this service. It implements the <code>TransactionManager</code> and the
 * <code>Administrable</code> interfaces.
 *
 * @author Sun Microsystems, Inc.
 * @since 1.1
 */
@com.gigaspaces.api.InternalApi
public class TxnMgrProxy implements ExtendedTransactionManager, Administrable, SmartExternalizable,
        ReferentUuid, MarshalPivotProvider {

    private static final long serialVersionUID = 2L;

    /**
     * The reference to the transaction manager service implementation
     *
     * @serial
     */
    TxnManager backend;

    TxnManager direct;

    /**
     * The proxy's <code>Uuid</code>
     *
     * @serial
     */
    public Uuid proxyID;


    /**
     * Creates a transaction manager proxy, returning an instance that implements
     * RemoteMethodControl if the server does too.
     *
     * @param mailbox the server proxy
     * @param id      the ID of the server
     */
    static TxnMgrProxy create(TxnManager txnMgr, Uuid id) {
        if (txnMgr instanceof RemoteMethodControl) {
            return new ConstrainableTxnMgrProxy(txnMgr, id, null);
        } else {
            return new TxnMgrProxy(txnMgr, id);
        }
    }

    public static TxnMgrProxy create(TxnManager txnMgr, TxnManager direct, Uuid id) {
        return new TxnMgrProxy(txnMgr, direct, id);
    }

    //create a proxy with no stub and same uuid and mgrid
    public TxnMgrProxy createLightProxy() {
        return new TxnMgrProxy(null, direct, proxyID);
    }

    // Just for serialization
    public TxnMgrProxy() {
    }

    private TxnMgrProxy(TxnManager txnMgr, Uuid id) {
        this(txnMgr, null, id);
    }

    /**
     * Convenience constructor.
     */
    private TxnMgrProxy(TxnManager txnMgr, TxnManager direct, Uuid id) {
        if (/*txnMgr == null ||*/  id == null) {
            throw new IllegalArgumentException("Cannot accept null arguments");
        }
        this.backend = txnMgr;
        this.direct = direct;
        this.proxyID = id;


    }


    public Created create(long lease)
            throws LeaseDeniedException, RemoteException {
        Created create = getProxy().create(lease);
        //Reattach removed stub from the lease, reduce unneeded network traffic and stub creation
        if (create.lease instanceof LandlordLease) {
            LandlordLease landlordLease = (LandlordLease) create.lease;
            landlordLease.setLandlord(this.backend);
        }
        return create;

    }


    public void join(long id, TransactionParticipant part, long crashCount)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        getProxy().join(id, part, crashCount);
    }

    public boolean
    disJoin(long id, TransactionParticipant part)
            throws UnknownTransactionException, RemoteException {
        return getProxy().disJoin(id, part);

    }


    public int getState(long id)
            throws UnknownTransactionException, RemoteException {
        return getProxy().getState(id);
    }

    public void commit(long id)
            throws UnknownTransactionException, CannotCommitException,
            RemoteException {
        getProxy().commit(id);
    }

    public void commit(long id, long waitFor)
            throws UnknownTransactionException, CannotCommitException,
            TimeoutExpiredException, RemoteException {
        getProxy().commit(id, waitFor);
    }

    public void abort(long id)
            throws UnknownTransactionException, CannotAbortException,
            RemoteException {
        getProxy().abort(id);
    }

    public void abort(long id, long waitFor)
            throws UnknownTransactionException, CannotAbortException,
            TimeoutExpiredException, RemoteException {
        getProxy().abort(id, waitFor);
    }

    public int prepare(Object xid)
            throws CannotCommitException, UnknownTransactionException, RemoteException {
        return getProxy().prepare(xid);

    }


    public void commit(Object xid)
            throws UnknownTransactionException, CannotCommitException,
            RemoteException {
        getProxy().commit(xid);

    }

    public void commit(Object xid, long waitFor)
            throws UnknownTransactionException, CannotCommitException,
            TimeoutExpiredException, RemoteException {
        getProxy().commit(xid, waitFor);

    }

    public void abort(Object xid)
            throws UnknownTransactionException, CannotAbortException,
            RemoteException {
        getProxy().abort(xid);

    }

    public void abort(Object xid, long waitFor)
            throws UnknownTransactionException, CannotAbortException,
            TimeoutExpiredException, RemoteException {
        getProxy().abort(xid, waitFor);

    }


    public Created create(Object xid, long lease)
            throws LeaseDeniedException, RemoteException {
        Created create = getProxy().create(xid, lease);
        //Reattach removed stub from the lease, reduce unneeded network traffic and stub creation
        if (create.lease instanceof LandlordLease) {
            LandlordLease landlordLease = (LandlordLease) create.lease;
            landlordLease.setLandlord(this.backend);
        }
        return create;
    }

    public int getState(Object xid)
            throws UnknownTransactionException, RemoteException {
        return getProxy().getState(xid);
    }

    public void join(Object id, TransactionParticipant part, long crashCount)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        getProxy().join(id, part, crashCount);
    }

    // used only in embedded mahalo- pass the user ServerTrasaction- allows
    // * updaing the lease interval in it 
    @Deprecated
    public void join(long id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (!userXtnObject.isEmbeddedMgrProxySideInstance())
            throw new UnsupportedOperationException(" supported only with embedded mahalo");
        getProxy().join(id, part, crashCount, userXtnObject);
    }

    // used only in embedded mahalo- pass the user ServerTrasaction- allows
    // * updaing the lease interval in it 
    @Deprecated
    public void join(Object id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (!userXtnObject.isEmbeddedMgrProxySideInstance())
            throw new UnsupportedOperationException(" supported only with embedded mahalo");
        getProxy().join(id, part, crashCount, userXtnObject);
    }

    public void
    join(long id, TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        getProxy().join(id, part, crashCount, partitionId, clusterName);
    }

    public void
    join(Object id, TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        getProxy().join(id, part, crashCount, partitionId, clusterName);
    }


    // used only in embedded mahalo- pass the user ServerTrasaction- allows
    // * updaing the lease interval in it 
    //also the space-proxy is passed and will be used in fail-over
    public void
    join(long id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject
            , int partitionId, String clusterName, Object clusterProxy)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (!userXtnObject.isEmbeddedMgrProxySideInstance())
            throw new UnsupportedOperationException(" supported only with embedded mahalo");
        getProxy().join(id, part, crashCount, userXtnObject, partitionId, clusterName, clusterProxy);
    }


    // used only in embedded mahalo- pass the user ServerTrasaction- allows
    // * updaing the lease interval in it 
    //also the space-proxy is passed and will be used in fail-over
    public void
    join(Object id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject
            , int partitionId, String clusterName, Object clusterProxy)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (!userXtnObject.isEmbeddedMgrProxySideInstance())
            throw new UnsupportedOperationException(" supported only with embedded mahalo");
        getProxy().join(id, part, crashCount, userXtnObject, partitionId, clusterName, clusterProxy);
    }


    /**
     * Retrieves a <code>Transaction</code> given the transaction's ID.
     *
     * @param xid the id
     */
    public void reenterPreparedExternalXid(Object xid, List<TransactionParticipant> parts)
            throws RemoteException, CannotCommitException {
        getProxy().reenterPreparedExternalXid(xid, parts);
    }


    // inherit javadoc from parent
    public Object getAdmin() throws RemoteException {
        return getProxy().getAdmin();
    }

    /* From net.jini.id.ReferentUuid */

    /**
     * Returns the universally unique identifier that has been assigned to the resource this proxy
     * represents.
     *
     * @return the instance of <code>Uuid</code> that is associated with the resource this proxy
     * represents. This method will not return <code>null</code>.
     * @see net.jini.id.ReferentUuid
     */
    public Uuid getReferentUuid() {
        return proxyID;
    }

    /**
     * Proxies for servers with the same proxyID have the same hash code.
     */
    @Override
    public int hashCode() {
        return proxyID.hashCode();
    }

    /**
     * Proxies for servers with the same <code>proxyID</code> are considered equal.
     */
    @Override
    public boolean equals(Object o) {
        //return ReferentUuids.compare(this,o);
        if (this == o)
            return true;
        if (o instanceof TxnMgrProxy) {
            TxnMgrProxy other = (TxnMgrProxy) o;
            return proxyID.getLeastSignificantBits() == other.proxyID.getLeastSignificantBits() &&
                    proxyID.getMostSignificantBits() == other.proxyID.getMostSignificantBits();
        } else
            return false;
    }


    public TxnManager getProxy() {
        if (direct != null) {
            return direct;
        }
        return backend;
    }

    /**
     * returns true if this mgr which requires that the txn participants join into the txn in
     * contrary to a mgr which the participants are known prior to txn propagation
     *
     * @return true if its a  mgr  which requires the txn participants to join
     */
    public boolean needParticipantsJoin() {
        return true;
    }

    public boolean isEmbeddedMgr() {
        return direct != null;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        if (backend != null) {
            out.writeBoolean(true);
            out.writeObject(backend);
        } else {
            out.writeBoolean(false);
        }
        out.writeLong(proxyID.getMostSignificantBits());
        out.writeLong(proxyID.getLeastSignificantBits());
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean())
            backend = (TxnManager) in.readObject();
        proxyID = new Uuid(in.readLong(), in.readLong());
    }

    /**
     * During deserialization of an instance of this class, if it is found that the stream contains
     * no data, this method is automatically invoked. Because it is expected that the stream should
     * always contain data, this implementation of this method simply declares that something must
     * be wrong.
     *
     * @throws <code>InvalidObjectException</code> to indicate that there was no data in the stream
     *                                             during deserialization of an instance of this
     *                                             class; declaring that something is wrong.
     */
    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("no data found when attempting to "
                + "deserialize TxnMgrProxy instance");
    }//end readObjectNoData


    /**
     * A subclass of TxnMgrProxy that implements RemoteMethodControl.
     */
    final static class ConstrainableTxnMgrProxy extends TxnMgrProxy
            implements RemoteMethodControl {
        private static final long serialVersionUID = 2L;

        /**
         * Required for Externalizable
         */
        public ConstrainableTxnMgrProxy() {
        }

        /**
         * Creates an instance of this class.
         */
        private ConstrainableTxnMgrProxy(TxnManager txnMgr, Uuid id,
                                         MethodConstraints methodConstraints) {
            super(constrainServer(txnMgr, methodConstraints),
                    id);
        }

        /**
         * Returns a copy of the server proxy with the specified client constraints and methods
         * mapping.
         */
        private static TxnManager constrainServer(
                TxnManager txnMgr,
                MethodConstraints methodConstraints) {
            return (TxnManager)
                    ((RemoteMethodControl) txnMgr).setConstraints(methodConstraints);
        }

        /**
         * {@inheritDoc}
         */
        public RemoteMethodControl setConstraints(
                MethodConstraints constraints) {
            return new ConstrainableTxnMgrProxy(backend, proxyID,
                    constraints);
        }

        /**
         * {@inheritDoc}
         */
        public MethodConstraints getConstraints() {
            return ((RemoteMethodControl) backend).getConstraints();
        }

        /* Note that the superclass's hashCode method is OK as is. */
        /* Note that the superclass's equals method is OK as is. */

        /**
         * Returns a proxy trust iterator that is used in <code>ProxyTrustVerifier</code> to
         * retrieve this object's trust verifier.
         */
        private ProxyTrustIterator getProxyTrustIterator() {
            return new SingletonProxyTrustIterator(backend);
        }//end getProxyTrustIterator

        /**
         * Performs various functions related to the trust verification process for the current
         * instance of this proxy class, as detailed in the description for this class.
         *
         * @throws <code>InvalidObjectException</code> if any of the requirements for trust
         *                                             verification (as detailed in the class
         *                                             description) are not satisfied.
         */
        private void readObject(ObjectInputStream s)
                throws IOException, ClassNotFoundException {
        /* Note that basic validation of the fields of this class was
             * already performed in the readObject() method of this class'
             * super class.
             */
            s.defaultReadObject();
            // Verify that the server implements RemoteMethodControl
            if (!(backend instanceof RemoteMethodControl)) {
                throw new InvalidObjectException(
                        "ConstrainableTxnMgrProxy.readObject failure - backend " +
                                "does not implement constrainable functionality ");
            }//endif
        }//end readObject 
    }

    public MarshalPivot getMarshalPivot() throws RemoteException {
        return new DefaultProxyPivot(backend);
    }

    @Override
    public String toString() {
        return "TxnMgrProxy [proxyId=" + proxyID.toString() + ", isDirect=" + (direct != null) + "]";
    }

    @Override
    public Uuid getTransactionManagerId() throws RemoteException {
        return proxyID;
    }
}
