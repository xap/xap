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
package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.logger.Constants;
import com.j_spaces.jdbc.AbstractDMLQuery;
import net.jini.core.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yohanakh
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class CollocatedJoinSpaceRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    private static final Logger _devLogger = LoggerFactory.getLogger(Constants.LOGGER_DEV);
    private Transaction txn;
    private int readModifier;
    private int max;
    private AbstractDMLQuery query;

    /**
     * Required for Externalizable.
     */
    public CollocatedJoinSpaceRequestInfo() {
    }

    /**
     * @param query
     * @param txn
     * @param readModifier
     * @param max
     */
    public CollocatedJoinSpaceRequestInfo(AbstractDMLQuery query, Transaction txn, int readModifier, int max) {
        this.query = query;
        this.txn = txn;
        this.readModifier = readModifier;
        this.max = max;
    }


    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, query);
        IOUtils.writeObject(out, txn);
        IOUtils.writeInt(out, readModifier);
        IOUtils.writeInt(out, max);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        query = IOUtils.readObject(in);
        txn = IOUtils.readObject(in);
        readModifier = IOUtils.readInt(in);
        max = IOUtils.readInt(in);
    }

    public AbstractDMLQuery getQuery() {
        return query;
    }

    public Transaction getTxn() {
        return txn;
    }

    public int getReadModifier() {
        return readModifier;
    }

    public int getMax() {
        return max;
    }
}
