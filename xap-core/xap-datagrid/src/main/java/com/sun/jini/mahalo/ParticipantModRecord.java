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
package com.sun.jini.mahalo;

import com.gigaspaces.logger.LogUtils;
import com.sun.jini.mahalo.log.CannotRecoverException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A <code>LogRecord</code> which encapsulates a generic interaction with a participant.
 *
 * @author Sun Microsystems, Inc.
 */
class ParticipantModRecord implements TxnLogRecord {
    static final long serialVersionUID = 5542043673924560855L;

    /**
     * Logger for operations related messages
     */
    private static final Logger operationsLogger =
            TxnManagerImpl.operationsLogger;

    /**
     * @serial
     */
    private ParticipantHandle part;

    /**
     * @serial
     */
    private int result;

    ParticipantModRecord(ParticipantHandle part, int result) {
        if (part == null)
            throw new IllegalArgumentException("ParticipantModRecord: " +
                    "recover: non-null ParticipantHandle " +
                    "recover attempted");

        this.part = part;
        this.result = result;
    }

    ParticipantHandle getPart() {
        return part;
    }

    int getResult() {
        return result;
    }

    public void recover(TxnManagerTransaction tmt)
            throws CannotRecoverException {
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.entering(operationsLogger, ParticipantModRecord.class, "recover", tmt);
        }
        if (tmt == null)
            throw new NullPointerException("ParticipantModRecord: recover: " +
                    "non-null transaction must be specified");

        tmt.modifyParticipant(getPart(), getResult());

        if (getResult() == ABORTED)
            tmt.modifyTxnState(ABORTED);

        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.exiting(operationsLogger, ParticipantModRecord.class, "recover");
        }
    }
}
