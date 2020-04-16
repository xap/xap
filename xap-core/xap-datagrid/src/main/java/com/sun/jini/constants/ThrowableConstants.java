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
package com.sun.jini.constants;

import net.jini.io.UnsupportedConstraintException;

import java.io.ObjectStreamException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.ProtocolException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.MarshalException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.ServerError;
import java.rmi.ServerException;
import java.rmi.UnexpectedException;
import java.rmi.UnknownHostException;
import java.rmi.UnmarshalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Various constants useful in processing exceptions
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class ThrowableConstants {

    final static Logger logger = LoggerFactory.getLogger("com.sun.jini.constants");

    /**
     * Value returned by <code>retryable</code> to indicate that the
     * passed <code>Throwable</code> does not provide any new
     * information on the state of the object that threw it.
     * @see #retryable
     */
    final static public int INDEFINITE = 0;

    /**
     * Value returned by <code>retryable</code> to indicate that the
     * passed <code>Throwable</code> implies that retrying the
     * operation that threw the <code>Throwable</code> with the same
     * arguments and the same expected return value would not be
     * fruitful.
     * @see #retryable
     */
    final static public int BAD_INVOCATION = 1;

    /**
     * Value returned by <code>retryable</code> to indicate that the
     * passed <code>Throwable</code> implies that any further
     * operations on the object that threw the <code>Throwable</code>
     * would not be fruitful.
     * @see #retryable
     */
    final static public int BAD_OBJECT = 2;

    /**
     * Value returned by <code>retryable</code> to indicate that the
     * passed <code>Throwable</code> was of a type that could not be
     * classified.
     * @see #retryable
     */
    final static public int UNCATEGORIZED = 3;

    /**
     * Attempt to classify the passed <code>Throwable</code> in terms of
     * what it implies about the probability of success of future operations
     * on the object that threw the exception. <p>
     *
     * Note, the classification used by this method tends to assume
     * the worst. For exceptions that represent conditions that could
     * get better by themselves but probably will not, it will return
     * <code>BAD_OBJECT</code> or <code>BAD_INVOCATION</code> instead
     * of <code>INDEFINITE</code>. This makes it suitable for
     * situations where it is better to give up, fail early, and
     * notify the next layer up that something is wrong than to
     * continue silently and retry. It is probably not a good choice
     * for situations where the stakes are higher, like deciding when
     * to give up on a prepared transaction.
     *
     * @return <code>INDEFINITE</code>, <code>BAD_INVOCATION</code>,
     * or <code>BAD_OBJECT</code> if the exception is a
     * <code>RuntimeException</code>, <code>Error</code>, or
     * <code>java.rmi.RemoteException</code> depending on the details of
     * the <code>Throwable</code>.  Otherwise return
     * <code>UNCATEGORIZED</code>
     * @throws NullPointerException if the passed <code>Throwable</code> is
     * <code>null</code>
     */
    public static int retryable(Throwable t) {
        if (logger.isTraceEnabled()) {
            logger.trace("Inside ThrowableConstants.retryable() ", t);
        }
        if (t == null)
            throw new NullPointerException("Must pass a non-null Throwable");

        if (t instanceof RuntimeException) {
            return BAD_INVOCATION;
        }

        if (t instanceof Error) {
            if ((t instanceof OutOfMemoryError)
                    || (t instanceof LinkageError)) {
                return INDEFINITE;
            }

            if (t instanceof StackOverflowError)
                return BAD_INVOCATION;

            return BAD_OBJECT;
        }

        if (t instanceof RemoteException) {
            final RemoteException re = (RemoteException) t;

            if (re instanceof NoSuchObjectException
                    || re instanceof UnexpectedException
                    || re instanceof UnknownHostException
                    || re instanceof ConnectException) {
                return BAD_OBJECT;
            }

            final Throwable detail = re.getCause();
            if (detail == null)
                return INDEFINITE;

            if (re instanceof MarshalException
                    || re instanceof UnmarshalException) {
                if (detail instanceof ObjectStreamException)
                    return BAD_INVOCATION;

                final int drs = retryable(detail);

                if (drs == BAD_OBJECT || drs == BAD_INVOCATION)
                    return BAD_INVOCATION;

                return INDEFINITE;
            }

            if (re instanceof ConnectIOException) {
                if (detail instanceof NoRouteToHostException
                        || detail instanceof PortUnreachableException
                        || detail instanceof ProtocolException) {
                    return BAD_OBJECT;
                }

                if (detail instanceof UnsupportedConstraintException
                        || detail instanceof ObjectStreamException) {
                    return BAD_INVOCATION;
                }

                return INDEFINITE;
            }

            if (re instanceof ServerException) {
                final int drs = retryable(detail);
                if (drs == BAD_OBJECT)
                    return BAD_INVOCATION;

                return drs;
            }

            if (re instanceof ServerError) {
                return retryable(detail);
            }

            return INDEFINITE;
        }

        return UNCATEGORIZED;

    }
}
