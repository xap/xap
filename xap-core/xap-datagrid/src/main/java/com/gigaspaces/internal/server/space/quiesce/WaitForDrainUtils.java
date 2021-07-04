package com.gigaspaces.internal.server.space.quiesce;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.filters.ReplicationStatistics;
import org.slf4j.Logger;

import java.util.concurrent.TimeoutException;

public class WaitForDrainUtils {

    public static void waitForDrain(SpaceImpl spaceImpl, long timeoutMs, long minTimeToWait, boolean backupOnly, Logger logger) throws TimeoutException {
        long start = System.currentTimeMillis();
        long remainingTime = timeoutMs;

        try {


            logger.info("Waiting for lease manager cycle to drain");
            remainingTime = tryWithinTimeout("Couldn't wait for drain - lease manager cycle timeout",
                    remainingTime, innerTimeout -> spaceImpl.getEngine().getLeaseManager().waitForNoCycleOnQuiesce(innerTimeout));

            logger.info("Waiting for lease manager transactions to drain");
            remainingTime = tryWithinTimeout("Couldn't wait for drain - timeout while waiting for transactions", remainingTime,
                    innerTimeout -> {
                        try {
                            return spaceImpl.getEngine().getTransactionHandler().waitForActiveTransactions(innerTimeout);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                    }
            );


            long currentDuration = System.currentTimeMillis() - start;
            if (currentDuration < minTimeToWait) {
                long timeToSleep = minTimeToWait - currentDuration;
                logger.info("Sleeping for [" + timeToSleep + "ms] to satisfy " + minTimeToWait);
                try {
                    Thread.sleep(timeToSleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TimeoutException("Interrupted while sleeping to satisfy minTimeToWait = " + minTimeToWait);
                }
            }

            repetitiveTryWithinTimeout("Backup is not synced", remainingTime, () -> backupOnly ? isBackupSynced(spaceImpl) : isAllTargetSync(spaceImpl, logger));
        } catch (TimeoutException e){
            logger.warn("Caught TimeoutException while waiting for "+spaceImpl.getContainerName()+" to drain",e);
            throw e;
        }
    }

    private static boolean isAllTargetSync(SpaceImpl spaceImpl, Logger logger) {
        logger.info("Waiting for all replication channels to drain");
        ReplicationStatistics.OutgoingReplication outGoingReplication = spaceImpl.getEngine().getReplicationNode().getAdmin().getStatistics().getOutgoingReplication();
        long lastKeyInRedoLog = outGoingReplication.getLastKeyInRedoLog();
        logger.info("last key in redolog = "+lastKeyInRedoLog);
        boolean allSynced = true;
        for (ReplicationStatistics.OutgoingChannel channel : outGoingReplication.getChannels()) {
            logger.info("channel "+channel.getTargetMemberName()+" last confirmed key = "+channel.getLastConfirmedKeyFromTarget());
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
