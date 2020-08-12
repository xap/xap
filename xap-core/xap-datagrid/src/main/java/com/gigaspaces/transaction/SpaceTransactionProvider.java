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
