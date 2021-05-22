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

/**
 *
 */

package com.j_spaces.core.multiple;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.multiple.query.QueryMultiplePartialFailureException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Thrown when one of the following space operations fails: <b>readMultiple,takeMultiple,clear.</b>
 *
 * <p>Thrown on: <ul> <li>Partial and complete failure. <li>Cluster/single space topologies.
 * <li>SQLQueries/Templates. </ul>
 *
 * <p>The exception contains: <ul> <li>An array of exceptions that caused it. One exception per each
 * space that failed. <li>An array of entries that were successfully read or take. </ul.
 *
 * <p> <b>Replaced {@link QueryMultiplePartialFailureException}.</b>
 *
 * </pre>
 *
 * @author anna
 * @since 7.1
 */

public class BatchOperationException
        extends QueryMultiplePartialFailureException implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private static final IEntryPacket[] _emptyResultsArray = new IEntryPacket[0];

    /**
     *
     */
    public BatchOperationException() {
        super();

    }

    /**
     * @param results
     * @param exceptions
     */
    public BatchOperationException(List results,
                                   List<Throwable> exceptions) {
        //some of the exception may be QueryMultiplePartialFailureException, consume such exceptions.
        List<Throwable> exs = consume(results, exceptions);
        setCauses(exs.toArray(new Exception[exs.size()]));
        setResults(results == null ? _emptyResultsArray : results.toArray());
    }

    /**
     * @param results
     * @param causes
     */
    public BatchOperationException(Object[] results, Throwable[] causes) {
        setResults(results);
        setCauses(causes);
    }

    /**
     *
     */
    public void clearResults() {
        if (getResults() == null)
            return;

        setResults(_emptyResultsArray);
    }

    private List<Throwable> consume(List results, List<Throwable> exceptions) {
        List<Throwable> exs = new LinkedList<Throwable>();
        for (Throwable e : exceptions) {
            if (e instanceof QueryMultiplePartialFailureException) {
                QueryMultiplePartialFailureException qmpfe = (QueryMultiplePartialFailureException) e;
                for (Object value : qmpfe.getResults())
                    results.add(value);
                for (Throwable t : qmpfe.getCauses())
                    exs.add(t);

            } else {
                exs.add(e);
            }
        }
        return exs;
    }


    /**
     * Return the main cause for this exception
     *
     * @return the main cause.
     */
    public Throwable getMajorityCause() {
        return getMajorityCause(getCauses());

    }

    /**
     * Return the original majority cause for this exception
     *
     * @return the main cause.
     */
    public Throwable getOriginalMajorityCause() {
        if (getCauses() == null || getCauses().length == 0)
            return null;
        Throwable[] originalCauses = new Throwable[getCauses().length];

        for (int i = 0; i < getCauses().length; i++) {
            Throwable originalCause = getCauses()[i];
            while (originalCause.getCause() != null && originalCause.getCause() != originalCause)
                originalCause = originalCause.getCause();

            originalCauses[i] = originalCause;
        }
        return getMajorityCause(originalCauses);

    }

    /**
     * Get the exception which type is the most common one.
     */
    private static Throwable getMajorityCause(Throwable[] causes) {
        if (causes == null || causes.length == 0)
            return null;

        HashMap<String, Integer> exceptionCount = new HashMap<String, Integer>();
        HashMap<String, Throwable> exceptionInstances = new HashMap<String, Throwable>();
        for (Throwable ex : causes) {
            String exClassName = ex.getClass().getName();
            if (!exceptionCount.containsKey(exClassName)) {
                exceptionCount.put(exClassName, 1);
                exceptionInstances.put(exClassName, ex);
            } else {
                int count = exceptionCount.get(exClassName);
                exceptionCount.put(exClassName, ++count);
            }
        }

        int majority = 0;
        Throwable majorityException = null;
        for (Map.Entry<String, Integer> countEntry : exceptionCount.entrySet()) {
            int value = countEntry.getValue();
            if (value > majority) {
                majority = value;
                majorityException = exceptionInstances.get(countEntry.getKey());
            }
        }
        return majorityException;

    }

    /**
     * @return true if given exception class is a direct cause of this exception
     */
    public boolean containsCause(Class<? extends Throwable> exceptionClass) {
        if (getCauses() == null)
            return false;

        for (Throwable ex : getCauses()) {
            if (exceptionClass.isAssignableFrom(ex.getClass())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getMessage() {
        Map<Class<? extends Throwable>, Integer> errorTypes = new HashMap<Class<? extends Throwable>, Integer>();
        Map<Class<? extends Throwable>, Throwable> errors = new HashMap<Class<? extends Throwable>, Throwable>();

        int failures;
        if (getCauses() == null) {
            failures = 0;
        } else {
            failures = getCauses().length;
            for (Throwable error : getCauses()) {
                Class<? extends Throwable> key = error.getClass();
                if (errorTypes.containsKey(key)) {
                    errorTypes.put(key, 1 + errorTypes.get(key));
                } else {
                    errorTypes.put(error.getClass(), 1);
                    errors.put(error.getClass(), error);
                }
            }
        }
        StringBuilder sb = new StringBuilder("Success:");
        sb.append(getResults() == null ? 0 : getResults().length);
        sb.append(", errors:").append(failures);
        sb.append(", [");
        for (Map.Entry<Class<? extends Throwable>, Integer> entry : errorTypes.entrySet()) {
            sb.append(entry.getKey());
            sb.append(':');
            sb.append(entry.getValue());
            sb.append("\nStackTrace: ");
            Throwable t = errors.get(entry.getKey());
            t.printStackTrace(new PrintWriter(new StringBuilderWriter(sb)));
            sb.append(' ');
        }
        sb.append(']');
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

    /* (non-Javadoc)
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        setResults(IOUtils.readEntryPacketArray(in));
        setCauses(IOUtils.readThrowableArray(in));
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObjectArray(out, getResults());
        IOUtils.writeObjectArray(out, getCauses());
    }
}
