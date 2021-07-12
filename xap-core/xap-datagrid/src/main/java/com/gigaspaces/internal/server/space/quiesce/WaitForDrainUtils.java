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
package com.gigaspaces.internal.server.space.quiesce;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.filters.ReplicationStatistics;
import com.j_spaces.core.transaction.TransactionHandler;
import net.jini.core.transaction.server.ServerTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public class WaitForDrainUtils {
    private static Logger logger = LoggerFactory.getLogger("com.gigaspaces.internal.server.space.quiesce.waitForDrain");

    public static void waitForDrain(SpaceImpl spaceImpl, long timeoutMs, long minTimeToWait, boolean isDemote, Logger customLogger) throws TimeoutException {
        final Logger loggerToUse = customLogger != null ? customLogger : logger;
        final String containerName = spaceImpl.getContainerName();
        long start = System.currentTimeMillis();
        long remainingTime = timeoutMs;

        loggerToUse.info("[{}]: Starting 'waitForDrain' process, start = {}, timeout = {}",containerName, start, timeoutMs);

        try {
            loggerToUse.info("[{}]: Waiting for lease manager cycle to drain",containerName);
            remainingTime = tryWithinTimeout("Timed out while waiting for drain - lease manager cycle timeout",
                    remainingTime, innerTimeout -> spaceImpl.getEngine().getLeaseManager().waitForNoCycleOnQuiesce(innerTimeout));

            loggerToUse.info("[{}]: Waiting for transactions to drain",containerName);
            remainingTime = tryWithinTimeout("Timed out while waiting for transactions to drain", remainingTime,
                    innerTimeout -> {
                        long startTxn = System.currentTimeMillis();
                        final TransactionHandler handler = spaceImpl.getEngine().getTransactionHandler();
                        boolean result;
                        try {
                            result = isDemote ? handler.waitForActiveTransactions(innerTimeout) : handler.waitForFinalizingTransactions(innerTimeout);
                        } catch (InterruptedException e) {
                            result = false;
                            Thread.currentThread().interrupt();
                        }
                        if(!result){
                            loggerToUse.warn("[{}]: Timeout elapsed while waiting for transactions to drain, remaining transactions: {} ",containerName, handler.getXtnTable().size());
                            if(loggerToUse.isTraceEnabled()){
                                StringBuilder builder = new StringBuilder("Not done waiting for transactions:");
                                for (Map.Entry<ServerTransaction, Long> entry : handler.getTimedXtns().entrySet()) {
                                    builder.append("\n[ txn id ").append(entry.getKey().getMetaData().getTransactionUniqueId()).append(" timeout in ").append(System.currentTimeMillis() - entry.getValue()).append(" ms ]");
                                }
                                loggerToUse.trace(builder.toString());
                            }
                        } else {
                            loggerToUse.info("[{}]: All transactions were drained, duration: "+ (System.currentTimeMillis() - startTxn) +" ms",containerName);
                        }
                        return result;
                    }
            );


            long currentDuration = System.currentTimeMillis() - start;
            if (currentDuration < minTimeToWait) {
                long timeToSleep = minTimeToWait - currentDuration;
                loggerToUse.info("[{}]: Sleeping for [" + timeToSleep + "ms] to satisfy " + minTimeToWait,containerName);
                try {
                    Thread.sleep(timeToSleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TimeoutException("Interrupted while sleeping to satisfy minTimeToWait = " + minTimeToWait);
                }
            }

            if(hasReplication(spaceImpl)) {
                loggerToUse.info("[{}]: Waiting for " + (isDemote ? "backup" : "all targets ") + " replication to drain", containerName);
                long startReplication = System.currentTimeMillis();
                repetitiveTryWithinTimeout(isDemote ? "Backup is not synced" : "Some targets are not synced", remainingTime, () -> isDemote ? isBackupSynced(spaceImpl) : isAllTargetSync(spaceImpl, loggerToUse));
                loggerToUse.info("[{}]: Replication drained, duration: " + (System.currentTimeMillis() - startReplication) + " ms", containerName);
            }
        } catch (TimeoutException e){
            loggerToUse.warn("[{}]: Caught TimeoutException while waiting for "+spaceImpl.getContainerName()+" to drain",e);
            throw e;
        }
    }


    private static boolean hasReplication(SpaceImpl spaceImpl) {
        return !spaceImpl.getEngine().getReplicationNode().getAdmin().getStatistics().getOutgoingReplication().getChannels().isEmpty();

    }
    private static boolean isAllTargetSync(SpaceImpl spaceImpl, Logger loggerToUse) {
        ReplicationStatistics.OutgoingReplication outGoingReplication = spaceImpl.getEngine().getReplicationNode().getAdmin().getStatistics().getOutgoingReplication();
        long lastKeyInRedoLog = outGoingReplication.getLastKeyInRedoLog();
        if(loggerToUse.isDebugEnabled()){
            loggerToUse.debug("[{}]: last key in redolog = {}",spaceImpl.getContainerName(),lastKeyInRedoLog);
        }
        boolean allSynced = true;
        for (ReplicationStatistics.OutgoingChannel channel : outGoingReplication.getChannels()) {
            if(loggerToUse.isDebugEnabled()){
                loggerToUse.debug("[{}]: channel "+channel.getTargetMemberName()+" last confirmed key = "+channel.getLastConfirmedKeyFromTarget());
            }
            allSynced = allSynced && channel.getLastConfirmedKeyFromTarget() == lastKeyInRedoLog;
        }
        return allSynced;
    }

    private static boolean isBackupSynced(SpaceImpl spaceImpl) {
        ReplicationStatistics.OutgoingReplication outGoingReplication = spaceImpl.getEngine().getReplicationNode().getAdmin().getStatistics().getOutgoingReplication();
        long lastKeyInRedoLog = outGoingReplication.getLastKeyInRedoLog();
        ReplicationStatistics.OutgoingChannel backupChannel = outGoingReplication.getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE).get(0);
        //Backup is synced
        return backupChannel.getLastConfirmedKeyFromTarget() == lastKeyInRedoLog;
    }

    private static long tryWithinTimeout(String msg, long timeoutMs, ParametrizedConditionProvider predicate) throws TimeoutException {
        long start = System.currentTimeMillis();
        if (!predicate.test(timeoutMs)) {
            throw new TimeoutException(msg);
        }

        long duration = System.currentTimeMillis() - start;
        long remainingTime = timeoutMs - duration;
        if (remainingTime < 0) {
            throw new TimeoutException(msg);
        }

        return remainingTime;
    }

    private static void repetitiveTryWithinTimeout(String msg, long timeoutMs, ConditionProvider f) throws TimeoutException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long currTime;
        while ((currTime = System.currentTimeMillis()) < deadline) {
            if (f.test()) return;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TimeoutException("Interrupted in sleep interval");
            }
        }

        throw new TimeoutException(msg);
    }

    private interface ConditionProvider {
        boolean test();
    }

    private interface ParametrizedConditionProvider {
        boolean test(long value);
    }
}
