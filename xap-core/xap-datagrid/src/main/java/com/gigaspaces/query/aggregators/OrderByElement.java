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
import com.gigaspaces.internal.query.RawEntry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Sagiv Michael
 * @since 16.0.0
 */

public class OrderByElement implements Externalizable, Comparable<OrderByElement> {

    private RawEntry rawEntry;
    private OrderByPath[] orderByPaths;
    private Object[]  orderByValues;
//    private HashMap<OrderByPath, Object> orderByPathsAndValuesMap;

    public OrderByElement(List<OrderByPath> orderByPaths, SpaceEntriesAggregatorContext context) {
        this.rawEntry = context.getRawEntry();
        this.orderByPaths = orderByPaths.toArray(new OrderByPath[0]);
        this.orderByValues = new Object[orderByPaths.size()];
        for (int i = 0; i < this.orderByPaths.length ; i++) {
            this.orderByValues[i] = context.getPathValue(this.orderByPaths[i].getPath());
        }

//        this.orderByPathsAndValuesMap = new HashMap<>();
//        for (OrderByPath orderByPath : orderByPaths) {
//            this.orderByPathsAndValuesMap.put(orderByPath, context.getPathValue(orderByPath.getPath()));
//        }
    }

    public OrderByElement() {
    }


    public RawEntry getRawEntry() {
        return rawEntry;
    }


    public Object getValue(OrderByPath orderByPath) {
        for (int i = 0; i < this.orderByPaths.length; i++) {
            if(this.orderByPaths[i].equals(orderByPath)) {
                return orderByValues[i];
            }
        }
        return null;

//        return orderByPathsAndValuesMap.get(orderByPath);
    }

    @Override
    public int compareTo(OrderByElement other) {
        int rc = 0;

        for (OrderByPath orderByPath : this.orderByPaths) {

            Comparable c1 = (Comparable) getValue(orderByPath);
            Comparable c2 = (Comparable) other.getValue(orderByPath);

            if (c1 == c2)
                continue;

            if (c1 == null)
                return orderByPath.isNullsLast() ? 1 : -1;

            if (c2 == null)
                return orderByPath.isNullsLast() ? -1 : 1;

            rc = c1.compareTo(c2);
            if (rc != 0)
                return orderByPath.getOrderBy() == OrderBy.DESC ? -rc : rc;

        }

        return rc;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, rawEntry);
        IOUtils.writeObjectArray(out, orderByPaths);
        IOUtils.writeObjectArray(out, orderByValues);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.rawEntry = (RawEntry) IOUtils.readObject(in);
        this.orderByPaths = readOrderByPathArray(in);
        this.orderByValues = IOUtils.readObjectArray(in);
    }


    private OrderByPath[] readOrderByPathArray(ObjectInput in) throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        if (length < 0) {
            return null;
        }
        final OrderByPath[] array = new OrderByPath[length];
        for (int i = 0; i < length; i++) {
            array[i] = IOUtils.readObject(in);
        }
        return array;
    }
}
