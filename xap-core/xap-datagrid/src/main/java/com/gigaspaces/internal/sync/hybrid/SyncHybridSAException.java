/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 * <p>
 * The software source code is proprietary and confidential information of GigaSpaces.
 * You may use the software source code solely under the terms and limitations of
 * The license agreement granted to you by GigaSpaces.
 *******************************************************************************/
package com.gigaspaces.internal.sync.hybrid;

import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.JSpaceUtilities;

/**
 * @author yaeln
 * @since 11.0.1
 */
public class SyncHybridSAException extends SAException {
    private static final long serialVersionUID = 8873735980686335039L;
    private final SyncHybridOperationDetails[] details;
    private SyncHybridExceptionOrigin exceptionOrigin;


    public SyncHybridSAException(Throwable t, SyncHybridOperationDetails[] syncHybridOperationsDetails, SyncHybridExceptionOrigin exceptionOrigin) {
        super(t);
        this.details = syncHybridOperationsDetails;
        this.exceptionOrigin = exceptionOrigin;

    }

    public SyncHybridOperationDetails[] getDetails() {
        return details;
    }

    /**
     * Introspects recursively the input Exception to obtain the SyncHybridSAException from the cause's hierarchy tree.
     * Note1: Will not handle compound exceptions like WriteMultipleException which contains an array of results
     * Note2: Will inspect only using getCause method, inner throwable which is linked via methods
     * like: getException, getThrowable, etc' will not be handled
     * @return null if there is no SyncHybridSAException in the cause's hierarchy tree, otherwise returns SyncHybridSAException inner cause.
     */
    public static SyncHybridSAException getSyncHybridException(Exception e) {
        return (SyncHybridSAException) JSpaceUtilities.getAssignableCauseExceptionFromHierarchy(e, SyncHybridSAException.class);
    }

    public SyncHybridExceptionOrigin getExceptionOrigin() {
        return exceptionOrigin;
    }
}
