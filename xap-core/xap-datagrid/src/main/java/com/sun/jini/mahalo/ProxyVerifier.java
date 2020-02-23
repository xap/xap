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

import com.gigaspaces.logger.LogUtils;
import com.sun.jini.landlord.ConstrainableLandlordLease;
import com.sun.jini.landlord.Landlord;
import com.sun.jini.landlord.LandlordProxyVerifier;

import net.jini.core.constraint.MethodConstraints;
import net.jini.core.constraint.RemoteMethodControl;
import net.jini.id.ReferentUuid;
import net.jini.id.Uuid;
import net.jini.security.TrustVerifier;
import net.jini.security.proxytrust.TrustEquivalence;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Defines a trust verifier for the smart proxies of a Mahalo server.
 */
final class ProxyVerifier implements TrustVerifier, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Logger for logging information about this instance
     */
    private static final Logger logger =
            Logger.getLogger("net.jini.security.trust");

    /**
     * The Mahalo server proxy.
     */
    private final RemoteMethodControl serverProxy;

    /**
     * The unique identifier associated with the backend server referenced by the
     * <code>serverProxy</code>, used for comparison with the IDs extracted from the smart proxies
     * being verified.
     *
     * @serial
     */
    private final Uuid proxyID;

    /**
     * Returns a verifier for the smart proxies of the specified Mahalo server proxy.
     *
     * @param serverProxy the Mahalo server proxy
     * @throws UnsupportedOperationException if <code>serverProxy</code> does not implement both
     *                                       {@link RemoteMethodControl} and {@link
     *                                       TrustEquivalence}
     */
    ProxyVerifier(TxnManager serverProxy, Uuid proxyID) {
        if (!(serverProxy instanceof RemoteMethodControl)) {
            throw new UnsupportedOperationException(
                    "No verifier available for non-constrainable service");
        } else if (!(serverProxy instanceof TrustEquivalence)) {
            throw new UnsupportedOperationException(
                    "Verifier requires service proxy to implement " +
                            "TrustEquivalence");
        } else if (proxyID == null) {
            throw new IllegalArgumentException(
                    "Proxy id cannot be null");
        }
        this.serverProxy = (RemoteMethodControl) serverProxy;
        this.proxyID = proxyID;
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean isTrustedObject(Object obj, TrustVerifier.Context ctx)
            throws RemoteException {
        if (logger.isLoggable(Level.FINER)) {
            LogUtils.entering(logger, ProxyVerifier.class, "isTrustedObject", new Object[]{obj, ctx});
        }
        if (obj == null || ctx == null) {
            throw new NullPointerException("Arguments must not be null");
        }
        RemoteMethodControl otherServerProxy;
        Uuid inputProxyID = null;
        if (obj instanceof TxnMgrProxy.ConstrainableTxnMgrProxy) {
            otherServerProxy = (RemoteMethodControl)
                    ((TxnMgrProxy) obj).backend;
            inputProxyID = ((ReferentUuid) obj).getReferentUuid();
        } else if (obj instanceof ConstrainableLandlordLease) {
            final LandlordProxyVerifier lpv =
                    new LandlordProxyVerifier((Landlord) serverProxy, proxyID);
            return lpv.isTrustedObject(obj, ctx);
        } else if (
                obj instanceof TxnMgrAdminProxy.ConstrainableTxnMgrAdminProxy) {
            otherServerProxy = (RemoteMethodControl)
                    ((TxnMgrAdminProxy) obj).server;
            inputProxyID = ((ReferentUuid) obj).getReferentUuid();
        } else if (obj instanceof TxnManager &&
                obj instanceof RemoteMethodControl) {
            otherServerProxy = (RemoteMethodControl) obj;
            inputProxyID = proxyID;
        } else {
            logger.log(Level.FINEST, "Object {0} is not a supported type",
                    obj);
            return false;
        }

        // For top-level proxies, quickly verify proxy Uuid
        if ((inputProxyID != null) &&
                !(proxyID.equals(inputProxyID))) {
            return false;
        }

        MethodConstraints mc = otherServerProxy.getConstraints();
        TrustEquivalence trusted =
                (TrustEquivalence) serverProxy.setConstraints(mc);
        boolean result = trusted.checkTrustEquivalence(otherServerProxy);
        if (logger.isLoggable(Level.FINER)) {
            LogUtils.exiting(logger, ProxyVerifier.class, "isTrustedObject", Boolean.valueOf(result));
        }
        return result;
    }
}
