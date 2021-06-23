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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class ScanningInfo implements SmartExternalizable {
    private static final long serialVersionUID = 7799080976017450924L;
    private Integer scanned;
    private Integer matched;

    public ScanningInfo() {
        this.scanned = 0;
        this.matched = 0;
    }

    public ScanningInfo(Integer scanned, Integer matched) {
        this.scanned = scanned;
        this.matched = matched;
    }

    public Integer getScanned() {
        return scanned;
    }

    public void setScanned(Integer scanned) {
        this.scanned = scanned;
    }

    public Integer getMatched() {
        return matched;
    }

    public void setMatched(Integer matched) {
        this.matched = matched;
    }

    @Override
    public String toString() {
        return "ScanningInfo{" +
                "scanned=" + scanned +
                ", matched=" + matched +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ScanningInfo)) return false;

        ScanningInfo that = (ScanningInfo) o;

        if (scanned != null ? !scanned.equals(that.scanned) : that.scanned != null) return false;
        return matched != null ? matched.equals(that.matched) : that.matched == null;

    }

    @Override
    public int hashCode() {
        int result = scanned != null ? scanned.hashCode() : 0;
        result = 31 * result + (matched != null ? matched.hashCode() : 0);
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(scanned);
        objectOutput.writeObject(matched);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.scanned = (Integer) objectInput.readObject();
        this.matched = (Integer) objectInput.readObject();
    }
}
