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

package com.gigaspaces.lrmi;

import com.gigaspaces.annotation.lrmi.OneWayRemoteCall;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.logger.Constants;

import java.rmi.Remote;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides repository of one-way remote methods.<br> If remote interface method doesn't
 * have explicit {@link OneWayRemoteCall} annotation definition or {@link OneWayRemoteCall}
 * annotation can't be defined for Remote interface due the thirty party provider. <br> Using this
 * class you can make registration of one-way method to the underlying transport protocol. <p>
 * <b>Important:<b> Best practice to register one-way methods before starting invoke this method
 * remotely or in static initializer or remote stub.
 *
 * @author Igor Goldenberg
 * @since 5.2
 **/
@com.gigaspaces.api.InternalApi
public class OneWayMethodRepository {
    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);

    final private static Set<IMethod> _repositoryTable = Collections.synchronizedSet(new HashSet<IMethod>());

    /**
     * Register one way remote method to the underlying transport protocol.<br> Important: The
     * declaring class of provided method must be an interface and extends from java.rmi.Remote
     * interface.
     *
     * @throws IllegalArgumentException The provided method is not instance of Remote interface.
     **/
    static public void register(IMethod method)
            throws IllegalArgumentException {
        if (!(method.getDeclaringClass().isInterface()))
            throw new IllegalArgumentException("The declaring class [" + method.getDeclaringClass() + "] of [" + method + "] method must be Remote interface.");

        if (!(Remote.class.isAssignableFrom(method.getDeclaringClass())))
            throw new IllegalArgumentException("The declaring class [" + method.getDeclaringClass() + "] of [" + method + "] method must extends from java.rmi.Remote interface.");

	 /* don't care if this method already exists, just replace the instance (save sync verifications) */
        _repositoryTable.add(method);

        if (_logger.isDebugEnabled()) {
            _logger.debug("OneWayMethodRepository - registered one-way method: " + method);
        }
    }


    /**
     * Check whether the {@link Method} registered as one-way to the underlying transport protocol.
     *
     * @param method the method to check.
     * @return <code>true</code> if the method registered as one-way, otherwise <code>false</code>.
     **/
    static public boolean isOneWay(IMethod method) {
        return _repositoryTable.contains(method);
    }
}
