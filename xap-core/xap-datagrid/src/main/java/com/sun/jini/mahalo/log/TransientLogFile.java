/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.mahalo.log;

import com.gigaspaces.logger.LogUtils;
import com.sun.jini.mahalo.TxnManager;
import com.sun.jini.mahalo.log.MultiLogManager.LogRemovalManager;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of a non-persistent <code>Log</code>.
 *
 * @author Sun Microsystems, Inc.
 * @see com.sun.jini.mahalo.log.Log
 */
@com.gigaspaces.api.InternalApi
public class TransientLogFile implements Log {
    /**
     * Unique ID associated with this log
     */
    private final long cookie;

    /**
     * Reference to <code>LogRemovalManager</code>, which is called to remove this log from the
     * managed set of logs.
     */
    private final LogRemovalManager logMgr;

    /**
     * Logger for persistence related messages
     */
    private static final Logger persistenceLogger =
            Logger.getLogger(TxnManager.MAHALO + ".persistence");

    /**
     * Logger for operations related messages
     */
    private static final Logger operationsLogger =
            Logger.getLogger(TxnManager.MAHALO + ".operations");

    /**
     * Simple constructor that simply assigns the given parameter to an internal field.
     *
     * @param id the unique identifier for this log
     * @see com.sun.jini.mahalo.log.Log
     * @see com.sun.jini.mahalo.log.LogManager
     * @see com.sun.jini.mahalo.log.MultiLogManager
     * @see com.sun.jini.mahalo.log.MultiLogManager.LogRemovalManager
     */
    public TransientLogFile(long id, LogRemovalManager lrm) {
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.entering(operationsLogger, TransientLogFile.class,"TransientLogFile", new Object[]{new Long(id), lrm});
        }
        cookie = id;
        logMgr = lrm;
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.exiting(operationsLogger, TransientLogFile.class, "TransientLogFile");
        }
    }

    /**
     * Returns the identifier associated with information in this <code>Log</code>.
     *
     * @see com.sun.jini.mahalo.log.Log
     */
    public long cookie() {
        return cookie;
    }

    /**
     * Add a <code>LogRecord</code> to the <code>Log</code>. This method does nothing with the
     * provided argument.
     *
     * @param rec the record to be ignored.
     * @see com.sun.jini.mahalo.log.LogRecord
     */
    public void write(LogRecord rec) throws LogException {
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.entering(operationsLogger, TransientLogFile.class, "write", rec);
        }
        if (persistenceLogger.isLoggable(Level.FINEST)) {
            persistenceLogger.log(Level.FINEST,
                    "(ignored) write called for cookie: {0}", new Long(cookie));
        }
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.exiting(operationsLogger, TransientLogFile.class, "write");
        }
    }

    /**
     * Invalidate the log.
     */
    public void invalidate() throws LogException {
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.entering(operationsLogger, TransientLogFile.class, "invalidate");
        }

        if (persistenceLogger.isLoggable(Level.FINEST)) {
            persistenceLogger.log(Level.FINEST,
                    "Calling logMgr to release cookie: {0}", new Long(cookie));
        }
        logMgr.release(cookie);

        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.exiting(operationsLogger, TransientLogFile.class, "invalidate");
        }
    }

    /**
     * Recover information from the log. Does nothing.
     *
     * @param client who to inform with information from the log.
     * @see com.sun.jini.mahalo.log.LogRecovery
     */
    public void recover(LogRecovery client) throws LogException {
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.entering(operationsLogger, MultiLogManager.class, "recover", client);
        }
        if (persistenceLogger.isLoggable(Level.FINEST)) {
            persistenceLogger.log(Level.FINEST,
                    "(ignored) Recovering for: {0}", new Long(cookie));
        }
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.exiting(operationsLogger, MultiLogManager.class, "recover");
        }
    }
}
