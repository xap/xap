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

package com.gigaspaces.exception.lrmi;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * An Application Exception abstracts a logical exception rather than an exception in network
 * infrastructure (which is abstracted by RemoteException).
 *
 * When the LRMIRuntime invoked() method is called (on the server side), it tries to invoke the
 * method on the designated object. If the method itself throws an exception, LRMIRuntime
 * encapsulates the exception in an application exception, and throws it back to the protocol
 * adapter. The protocol adapter should transmit the exception to its client peer and throw the
 * exception to the application.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class ApplicationException extends ExecutionException implements SmartExternalizable {
    private static final long serialVersionUID = 2L;

    /**
     * For Externalizable
     */
    public ApplicationException() {
    }

    public ApplicationException(String msg, Throwable applExc) {
        super(msg, applExc);
    }

    // no need to store the stack trace for this exception
    @Override
    public Throwable fillInStackTrace() {
        return null;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getCause());
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        initCause((Throwable) in.readObject());
    }
}