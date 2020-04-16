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

import com.sun.jini.mahalo.log.CannotRecoverException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An <code>AbortRecord</code> represents the logged state of a <code>Transaction</code> which has
 * changed to the ABORTED state.
 *
 * @author Sun Microsystems, Inc.
 */
class AbortRecord implements TxnLogRecord {
    /**
     * @serial
     */
    private ParticipantHandle[] parts;

    static final long serialVersionUID = -8121722031382234695L;

    static final Logger logger = TxnManagerImpl.participantLogger;

    /**
     * Constructs an <code>AbortRecord</code> which  represents a <code>Transaction</code> which has
     * moved to the ABORTED state.
     *
     * @param parts The array of participants joined in the transaction
     * @see net.jini.core.transaction.Transaction
     * @see net.jini.core.transaction.server.TransactionParticipant
     * @see net.jini.core.transaction.server.TransactionConstants
     */
    AbortRecord(ParticipantHandle[] parts) {
        if (parts == null)
            throw new IllegalArgumentException("AbortRecord: must specify " +
                    "a non-null parts array");
        this.parts = parts;
    }

    /**
     * Recovers the state encapsulated the <code>AbortRecord</code> to the caller.
     *
     * @param tmt The <code>TxnManagerTransaction</code> to which state is recovered.
     * @see com.sun.jini.mahalo.TxnManagerTransaction
     */
    public void recover(TxnManagerTransaction tmt)
            throws CannotRecoverException {
        try {
            for (int i = 0; i < parts.length; i++) {
                tmt.add(parts[i]);
            }
            tmt.modifyTxnState(ABORTED);
        } catch (InternalManagerException ime) {
            throw new CannotRecoverException("AbortRecord: recover: " +
                    ime.getMessage());
        }

        if (logger.isTraceEnabled()) {
            logger.trace("AbortJob:recover recovered");
        }
    }

    /**
     * Retrieves the set of <code>TransactionParticipant</code>s associated with the recovered
     * <code>Transaction</code>.
     */
    ParticipantHandle[] getParts() {
        return parts;
    }
}
