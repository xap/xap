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
import com.sun.jini.thread.RetryTask;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.WakeupManager;

import net.jini.core.transaction.server.TransactionParticipant;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A <code>ParticipantTask</code> is a general task which interacts with a participant.
 *
 * @author Sun Microsystems, Inc.
 * @see TransactionParticipant
 * @see TaskManager
 */
@com.gigaspaces.api.InternalApi
public class ParticipantTask extends RetryTask {
    final private ParticipantHandle handle;
    final private Job myjob;
    private static final Logger operationsLogger =
            TxnManagerImpl.operationsLogger;

    /**
     * Constructs a <code>ParticipantTask</code>.
     *
     * @param manager <code>TaskManager</code> providing the threads of execution.
     * @param myjob   <code>Job</code> to which this task belongs.
     * @param handle  <code>ParticipantHandle</code> representing the <code>TransactionParticipant</code>
     *                with which this task interacts.
     */
    public ParticipantTask(TaskManager manager, WakeupManager wm,
                           Job myjob, ParticipantHandle handle) {
        super(manager, wm);
        this.myjob = myjob;
        this.handle = handle;
    }

    /**
     * Inherit doc comment from supertype.
     *
     * @see com.sun.jini.thread.RetryTask
     */

    public boolean runAfter(List list, int max) {
        return false;
    }

    public boolean tryOnce() {
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.entering(operationsLogger, ParticipantTask.class, "tryOnce");
        }

        boolean result = false;
        try {
            result = myjob.performWork(this, handle);
        } catch (UnknownTaskException ute) {
            //If task doesn't belong to the
            //Job, then stop doing work.
            result = true;
        } catch (JobException je) {
            je.printStackTrace();
        }
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.exiting(operationsLogger, ParticipantTask.class, "tryOnce", Boolean.valueOf(result));
        }

        return result;
    }
}
