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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class WriteEntriesResult implements SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    private WriteEntryResult[] _results;
    private Exception[] _errors;

    /**
     * Required for Externalizable
     */
    public WriteEntriesResult() {
    }

    public WriteEntriesResult(int length) {
        this._results = new WriteEntryResult[length];
    }

    public int getSize() {
        return _results.length;
    }

    public void setResult(int i, WriteEntryResult result) {
        _results[i] = result;
    }

    public boolean isError(int i) {
        return _errors != null && _errors[i] != null;
    }

    public void setError(int i, Exception exception) {
        if (_errors == null)
            _errors = new Exception[_results.length];
        _errors[i] = exception;
    }

    public Exception[] getErrors() {
        return _errors;
    }

    public WriteEntryResult[] getResults() {
        return _results;
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("results", _results);
        textualizer.append("errors", _errors);
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        IOUtils.writeObject(out, _results);
        IOUtils.writeObject(out, _errors);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        this._results = IOUtils.readObject(in);
        this._errors = IOUtils.readObject(in);
    }
}
