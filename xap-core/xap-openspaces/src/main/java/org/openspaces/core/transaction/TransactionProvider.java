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


package org.openspaces.core.transaction;

import com.j_spaces.core.IJSpace;

import net.jini.core.transaction.Transaction;

import java.io.Closeable;

/**
 * A transaction provider is used to support declarative transactions. It is usually used with a
 * {@link org.openspaces.core.GigaSpace} implementation to declaratively provide on going
 * transactions to JavaSpace APIs.
 *
 * <p> The transaction provider usually interacts with a transaction manager (for example, Spring's
 * {@link org.springframework.transaction.PlatformTransactionManager}) in order to get the current
 * running transactions. It allows a transactional context to be passed to the current transaction
 * method, though it doesn't have to be used.
 *
 * @author kimchy
 */
public interface TransactionProvider extends Closeable {

    /**
     * Returns the currently running transaction (usually managed externally/declarative). A
     * transactional context can be passed and optionally used in order to fetch the current running
     * transaction.
     *
     * <p> If no transaction is currently executing, <code>null</code> value will be returned. This
     * usually means that the operation will be performed without a transaction.
     *
     * @param transactionalContext Transactional context to (optionally) fetch the transaction by
     * @param space                The actual Space this operation will be performed on
     * @return The transaction object to be used with {@link com.j_spaces.core.IJSpace} operations.
     * Can be <code>null</code>.
     */
    Transaction.Created getCurrentTransaction(Object transactionalContext, IJSpace space);

    /**
     * Returns the currently running transaction isolation level (mapping to Spring {@link
     * org.springframework.transaction.TransactionDefinition#getIsolationLevel()} values). A
     * transactional context can be passed and optionally used in order to fetch the current running
     * transaction.
     *
     * @param transactionalContext Transactional context to (optionally) fetch the transaction by
     * @return The transaction isolation level mapping to Spring {@link org.springframework.transaction.TransactionDefinition#getIsolationLevel()}.
     */
    int getCurrentTransactionIsolationLevel(Object transactionalContext);

    /**
     * Returns <code>true</code> if this transaction provider is enabled or not.
     */
    boolean isEnabled();
}
