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
package com.gigaspaces.transaction;

import com.gigaspaces.client.IsolationLevelModifiers;
import net.jini.core.transaction.Transaction;

import java.io.Closeable;

/**
 * A transaction provider for space proxy.
 *
 * @author Niv Ingberg
 * @since 12.2
 */
public interface SpaceTransactionProvider extends Closeable {

    /**
     * Returns <code>true</code> if this transaction provider is enabled or not.
     */
    boolean isEnabled();

    /**
     * Returns the currently running transaction (usually managed externally/declarative).
     *
     * <p> If no transaction is currently executing, <code>null</code> value will be returned. This
     * usually means that the operation will be performed without a transaction.
     *
     * @return The transaction object to be used (can be <code>null</code>).
     */
    Transaction getCurrentTransaction();

    /**
     * Returns the currently running transaction isolation level.
     *
     * @return The transaction isolation level.
     */
    IsolationLevelModifiers getCurrentTransactionIsolationLevel();
}
