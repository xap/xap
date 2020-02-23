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
package com.sun.jini.reggie;

import com.sun.jini.action.GetBooleanAction;
import com.sun.jini.proxy.MarshalledWrapper;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.security.Security;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.MarshalException;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.RemoteObject;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Item contains the fields of a ServiceItem packaged up for transmission between client-side
 * proxies and the registrar server. Instances are never visible to clients, they are private to the
 * communication between the proxies and the server. <p> This class only has a bare minimum of
 * methods, to minimize the amount of code downloaded into clients.
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class Item implements Serializable, Cloneable {

    private static final long serialVersionUID = 2L;

    /**
     * Logger for Reggie.
     */
    private static final Logger logger =
            Logger.getLogger("com.sun.jini.reggie");
    /**
     * Flag to enable JRMP impl-to-stub replacement during marshalling of service proxy.
     */
    private static final boolean enableImplToStubReplacement;

    static {
        Boolean b;
        try {
            b = (Boolean) Security.doPrivileged(new GetBooleanAction(
                    "com.sun.jini.reggie.enableImplToStubReplacement"));
        } catch (SecurityException e) {
            logger.log(Level.FINE, "failed to read system property", e);
            b = Boolean.FALSE;
        }
        enableImplToStubReplacement = b.booleanValue();
    }

    /**
     * ServiceItem.serviceID.
     *
     * @serial
     */
    public ServiceID serviceID;
    /**
     * The Class of ServiceItem.service converted to ServiceType.
     *
     * @serial
     */
    public ServiceType serviceType;
    /**
     * The codebase of the service object.
     *
     * @serial
     */
    public String codebase;
    /**
     * ServiceItem.service as a MarshalledWrapper.
     *
     * @serial
     */
    public MarshalledWrapper service;
    /**
     * ServiceItem.attributeSets converted to EntryReps.
     *
     * @serial
     */
    public EntryRep[] attributeSets;

    /**
     * Converts a ServiceItem to an Item.  Any exception that results is bundled up into a
     * MarshalException.
     */
    public Item(ServiceItem item) throws RemoteException {
        Object svc = item.service;
        if (enableImplToStubReplacement && svc instanceof Remote) {
            try {
                svc = RemoteObject.toStub((Remote) svc);
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "replacing {0} with {1}",
                            new Object[]{item.service, svc});
                }
            } catch (NoSuchObjectException e) {
            }
        }
        serviceID = item.serviceID;
        ServiceTypeBase stb = ClassMapper.toServiceTypeBase(svc.getClass());
        serviceType = stb.type;
        codebase = stb.codebase;
        try {
            service = new MarshalledWrapper(svc);
        } catch (IOException e) {
            throw new MarshalException("error marshalling arguments", e);
        }
        attributeSets = EntryRep.toEntryRep(item.attributeSets, true);
    }

    /**
     * Convert back to a ServiceItem.  If the service object cannot be constructed, it is set to
     * null.  If an Entry cannot be constructed, it is set to null.  If a field of an Entry cannot
     * be unmarshalled, it is set to null.
     */
    public ServiceItem get() {
        Object obj = null;
        try {
            obj = service.get();
        } catch (Throwable e) {
            RegistrarProxy.handleException(e);
        }
        return new ServiceItem(serviceID,
                obj,
                EntryRep.toEntry(attributeSets));
    }

    /**
     * Deep clone.  This is really only needed in the server, but it's very convenient to have
     * here.
     */
    public Object clone() {
        try {
            Item item = (Item) super.clone();
            EntryRep[] attrSets = item.attributeSets.clone();
            for (int i = attrSets.length; --i >= 0; ) {
                attrSets[i] = (EntryRep) attrSets[i].clone();
            }
            item.attributeSets = attrSets;
            return item;
        } catch (CloneNotSupportedException e) {
            throw new InternalError();
        }
    }

    /**
     * Converts an ArrayList of Item to an array of ServiceItem.
     */
    public static ServiceItem[] toServiceItem(ArrayList reps) {
        ServiceItem[] items = null;
        if (reps != null) {
            items = new ServiceItem[reps.size()];
            for (int i = items.length; --i >= 0; ) {
                items[i] = ((Item) reps.get(i)).get();
            }
        }
        return items;
    }
}
