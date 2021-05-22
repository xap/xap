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
package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Duration;

public class TieredStorageTableConfig implements SmartExternalizable {
    private String name;
    private String timeColumn;
    private Duration retention;
    private Duration period;
    private String criteria;
    private boolean isTransient;

    public TieredStorageTableConfig() {
    }

    public String getName() {
        return name;
    }

    public TieredStorageTableConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    public TieredStorageTableConfig setTimeColumn(String timeColumn) {
        this.timeColumn = timeColumn;
        return this;
    }

    public Duration getRetention() {
        return retention;
    }

    public TieredStorageTableConfig setRetention(Duration retention) {
        this.retention = retention;
        return this;
    }

    public Duration getPeriod() {
        return period;
    }

    public TieredStorageTableConfig setPeriod(Duration period) {
        this.period = period;
        return this;
    }

    public String getCriteria() {
        return criteria;
    }

    public TieredStorageTableConfig setCriteria(String criteria) {
        this.criteria = criteria;
        return this;
    }

    public boolean isTransient() {
        return isTransient;
    }

    public TieredStorageTableConfig setTransient(boolean aTransient) {
        isTransient = aTransient;
        return this;
    }

    public boolean isTimeRule(){
        return timeColumn != null && period != null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, name);
        IOUtils.writeString(out, timeColumn);
        IOUtils.writeObject(out, retention);
        IOUtils.writeObject(out, period);
        IOUtils.writeString(out, criteria);
        out.writeBoolean(isTransient);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.name = IOUtils.readString(in);
        this.timeColumn = IOUtils.readString(in);
        this.retention = IOUtils.readObject(in);
        this.period = IOUtils.readObject(in);
        this.criteria = IOUtils.readString(in);
        this.isTransient = in.readBoolean();
    }

    @Override
    public String toString() {
        return "TieredStorageTableConfig{" +
                "name='" + name + '\'' +
                ", timeColumn='" + timeColumn + '\'' +
                ", retention=" + retention +
                ", period=" + period +
                ", criteria='" + criteria + '\'' +
                ", isTransient=" + isTransient +
                '}';
    }
}
