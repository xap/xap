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

package com.gigaspaces.query.aggregators;

import com.gigaspaces.internal.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by anna on 11/10/14.
 */
@com.gigaspaces.api.InternalApi
public class OrderByPath implements Externalizable {

    private static final long serialVersionUID = 1L;


    private String path;
    private OrderBy orderBy;
    private boolean nullsLast;

    public OrderByPath() {
    }

    public OrderByPath(String path, OrderBy orderBy, boolean nullsLast) {
        this.path = path;
        this.orderBy = orderBy;
        this.nullsLast = nullsLast;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public boolean isNullsLast() {
        return nullsLast;
    }

    public void setNullsLast(boolean nullsLast) {
        this.nullsLast = nullsLast;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, path);
        IOUtils.writeObject(out, orderBy);
        out.writeBoolean(isNullsLast());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        path = IOUtils.readString(in);
        orderBy = IOUtils.readObject(in);
        nullsLast = in.readBoolean();
    }

    @Override
    public String toString() {
        return path + " " + orderBy + " " + (nullsLast ? "NULLS LAST" : "NULLS FIRST");
    }
}
