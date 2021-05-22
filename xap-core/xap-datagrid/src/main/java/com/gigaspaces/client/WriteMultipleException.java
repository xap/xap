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


package com.gigaspaces.client;

import com.gigaspaces.internal.exceptions.WriteResultImpl;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.multiple.write.WriteMultiplePartialFailureException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

/**
 * Thrown when writeMultiple operation fails.
 *
 * <p>Thrown on: <ul> <li>Partial and complete failure. <li>Cluster/single space topologies. </ul>
 *
 * <p>The exception contains an array of write results where each result in the array is either a
 * lease or an exception upon failure, the result index corresponds to the entry index in the array
 * of entries which are being written/updated.
 *
 * <p> <b>Replaced {@link WriteMultiplePartialFailureException}.</b>
 *
 * </pre>
 *
 * @author eitany
 * @since 7.1
 */

public class WriteMultipleException extends WriteMultiplePartialFailureException implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    public static interface IWriteResult extends com.j_spaces.core.multiple.write.IWriteResult {
    }

    /**
     * Default constructor required by Externalizable.
     */
    public WriteMultipleException() {
        super();
    }

    /**
     * @param results array of results values to be sent to user.
     */
    public WriteMultipleException(IWriteResult[] results) {
        super();
        _results = results;
    }

    @Override
    public IWriteResult[] getResults() {
        return _results;
    }

    /**
     * @param length the length of results array.
     * @param e      an error value to be used for each value sent to the user.
     */
    public WriteMultipleException(int length, Throwable e) {
        super();
        _results = new IWriteResult[length];
        for (int i = 0; i < _results.length; i++)
            _results[i] = WriteResultImpl.createErrorResult(e);
    }

    /**
     * Summarize how many success and fail there are for the request, show how much failures for
     * each type of error.
     */
    @Override
    public String getMessage() {
        Map<Class<? extends Throwable>, Integer> errorTypes = new HashMap<Class<? extends Throwable>, Integer>();
        Map<Class<? extends Throwable>, Throwable> errors = new HashMap<Class<? extends Throwable>, Throwable>();

        int resultsCounter = 0;
        int failures = 0;
        for (IWriteResult result : _results) {
            if (result == null) {
                if (errorTypes.containsKey(Error.class)) {
                    errorTypes.put(Error.class, 1 + errorTypes.get(Error.class));
                } else {
                    errorTypes.put(Error.class, 1);
                }
                ++failures;
            } else if (result.getResultType() == IWriteResult.ResultType.ERROR) {
                Class<? extends Throwable> key = result.getError().getClass();
                if (errorTypes.containsKey(key)) {
                    errorTypes.put(key, 1 + errorTypes.get(key));
                } else {
                    errorTypes.put(result.getError().getClass(), 1);
                    errors.put(result.getError().getClass(), result.getError());
                }
                ++failures;
            } else {
                ++resultsCounter;
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Success:").append(resultsCounter);
        sb.append(", errors:").append(failures);
        sb.append(", [");
        for (Map.Entry<Class<? extends Throwable>, Integer> entry : errorTypes.entrySet()) {
            sb.append(entry.getKey());
            sb.append(":");
            sb.append(entry.getValue());
            sb.append("\nStackTrace: ");
            Throwable t = errors.get(entry.getKey());
            t.printStackTrace(new PrintWriter(new StringBuilderWriter(sb)));
            sb.append(" ");
        }
        sb.append("]");
        return sb.toString();
    }

    static private class StringBuilderWriter extends Writer {

        final private StringBuilder builder;

        public StringBuilderWriter(StringBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            builder.append(cbuf, off, len);
        }
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _results = (IWriteResult[]) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_results);
    }
}
