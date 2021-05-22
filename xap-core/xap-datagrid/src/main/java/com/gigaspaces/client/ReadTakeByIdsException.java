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

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;


/**
 * A base class for Read/TakeByIdsException
 *
 * @author idan
 * @since 7.1.1
 */
public abstract class ReadTakeByIdsException extends RuntimeException implements SmartExternalizable {

    private static final long serialVersionUID = 1L;
    protected ReadTakeByIdResult[] _results;

    public ReadTakeByIdResult[] getResults() {
        return _results;
    }

    /**
     * Default constructor required by Externalizable.
     */
    public ReadTakeByIdsException() {
    }

    public static ReadTakeByIdsException newException(ReadTakeByIdResult[] results, boolean takeOperation) {
        if (takeOperation) {
            return new TakeByIdsException(results);
        }
        return new ReadByIdsException(results);
    }

    public static ReadTakeByIdsException newException(Object[] ids, Object[] results, Throwable[] exceptions, boolean takeOperation) {
        ReadTakeByIdResult[] exceptionResults = new ReadTakeByIdResult[ids.length];
        for (int i = 0; i < ids.length; i++) {
            exceptionResults[i] = new ReadTakeByIdResult(ids[i], results[i], exceptions[i]);
        }
        if (takeOperation) {
            return new TakeByIdsException(exceptionResults);
        }
        return new ReadByIdsException(exceptionResults);
    }

    public static ReadTakeByIdsException newException(Object[] ids, Throwable t, boolean takeOperation) {
        ReadTakeByIdResult[] results = new ReadTakeByIdResult[ids.length];
        for (int i = 0; i < results.length; i++) {
            results[i] = new ReadTakeByIdResult(ids[i], null, t);
        }
        return newException(results, takeOperation);
    }

    public ReadTakeByIdsException(ReadTakeByIdResult[] results) {
        _results = results;
    }

    public ReadTakeByIdsException(Object[] ids, Object[] results, Throwable[] exceptions) {
        constructResultsArray(ids, results, exceptions);
    }

    protected void constructResultsArray(Object[] ids, Object[] results, Throwable[] exceptions) {
        _results = new ReadTakeByIdResult[results.length];
        for (int i = 0; i < results.length; i++) {
            _results[i] = new ReadTakeByIdResult(ids[i], results[i], exceptions[i]);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_results);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _results = (ReadTakeByIdResult[]) in.readObject();
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
        int errorsCounter = 0;
        for (ReadTakeByIdResult result : _results) {
            if (result.isError()) {
                Class<? extends Throwable> key = result.getError().getClass();
                if (errorTypes.containsKey(key)) {
                    errorTypes.put(key, 1 + errorTypes.get(key));
                } else {
                    errorTypes.put(result.getError().getClass(), 1);
                    errors.put(result.getError().getClass(), result.getError());
                }
                errorsCounter++;
            } else {
                resultsCounter++;
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Success:").append(resultsCounter);
        sb.append(", errors:").append(errorsCounter);
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

    private static class StringBuilderWriter extends Writer {
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


}