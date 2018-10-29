package com.gigaspaces.internal.server.space.demote;

import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.filters.ReplicationStatistics;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.Engine.ENGINE_DEMOTE_MIN_TIMEOUT;
import static com.j_spaces.core.Constants.Engine.ENGINE_DEMOTE_MIN_TIMEOUT_DEFAULT;
import static com.j_spaces.core.Constants.LeaderSelector.LEADER_SELECTOR_HANDLER_CLASS_NAME;


/**
 * @author Yohana Khoury
 * @since 14.0
 */
@com.gigaspaces.api.InternalApi
public class DemoteHandler implements ISpaceModeListener {
    private final Logger _logger;
    private final SpaceImpl _spaceImpl;
    private final AtomicBoolean _isDemoteInProgress = new AtomicBoolean(false);
    private volatile CountDownLatch _latch;
    private final long _demoteMinTimeout;
    private final static String MIN_TIME_TO_DEMOTE_IN_MS = ENGINE_DEMOTE_MIN_TIMEOUT;

    public DemoteHandler(SpaceImpl spaceImpl) {
        _spaceImpl = spaceImpl;
        _logger = Logger.getLogger(Constants.LOGGER_DEMOTE + '.' + spaceImpl.getNodeName());
        _demoteMinTimeout = StringUtils.parseDurationAsMillis(_spaceImpl.getConfigReader().getSpaceProperty(MIN_TIME_TO_DEMOTE_IN_MS, ENGINE_DEMOTE_MIN_TIMEOUT_DEFAULT));
    }

    public void demote(long timeout, TimeUnit unit) throws DemoteFailedException {
        if (unit.toMillis(timeout) < _demoteMinTimeout) {
            throw new DemoteFailedException("Timeout must be equal or greater than " + MIN_TIME_TO_DEMOTE_IN_MS + "=" + _demoteMinTimeout + "ms");
        }


        if (!_isDemoteInProgress.compareAndSet(false, true)) {
            throw new DemoteFailedException("Demote is already in progress");
        }

        try {
            validationChecks();

            _spaceImpl.addInternalSpaceModeListener(this);
            _latch = new CountDownLatch(1);
            demoteImpl(timeout, unit);
        } catch (TimeoutException e) {
            throw new DemoteFailedException(e);
        } finally {
            _spaceImpl.removeInternalSpaceModeListener(this);
            _isDemoteInProgress.set(false);
        }
    }


    private long tryWithinTimeout(String msg, long timeoutMs, LongPredicate predicate) throws TimeoutException {
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

    private long repetitiveTryWithinTimeout(String msg, long timeoutMs, BooleanSupplier f) throws TimeoutException, DemoteFailedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long currTime;
        while ((currTime = System.currentTimeMillis()) < deadline) {
            if (f.getAsBoolean()) return deadline - currTime;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DemoteFailedException("Demote got interrupted");
            }
        }

        throw new TimeoutException(msg);
    }

    private void demoteImpl(long timeout, TimeUnit timeoutUnit) throws DemoteFailedException, TimeoutException {
        long start = System.currentTimeMillis();
        long timeoutMs = timeoutUnit.toMillis(timeout);

        try {

            _logger.info("Demoting to backup, entering quiesce mode...");
            _spaceImpl.getQuiesceHandler().quiesceDemote("Space is demoting from primary to backup");


            long remainingTime = timeoutMs;
            remainingTime = tryWithinTimeout("Couldn't demote to backup - lease manager cycle timeout", remainingTime,
                    _spaceImpl.getEngine().getLeaseManager()::waitForNoCycleOnQuiesce);


            remainingTime = tryWithinTimeout("Couldn't demote to backup - timeout while waiting for transactions", remainingTime,
                    this::waitForActiveTransactions);


            //Sleep remaining time to minTimeToDemoteInMs
            //_logger
            long currentDuration = System.currentTimeMillis() - start;
            if (currentDuration < _demoteMinTimeout) {
                long timeToSleep = _demoteMinTimeout - currentDuration;
                _logger.info("Sleeping for ["+timeToSleep+"] to satisfy " + MIN_TIME_TO_DEMOTE_IN_MS);
                try {
                    Thread.sleep(timeToSleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DemoteFailedException("Demote got interrupted");
                }
            }

            repetitiveTryWithinTimeout("Backup is not synced", remainingTime, this::isBackupSynced);


            validateSpaceStatus(false);

            //close outgoing connections
            closeOutgoingChannels();


            //Restart leader selector handler (in case of ZK then it restarts
            // the connection to ZK so the backup becomes primary)
            if (!_spaceImpl.restartLeaderSelectorHandler()) {
                throw new DemoteFailedException("Could not restart leader selector");
            }

            try {
                boolean succeeded = _latch.await(5, TimeUnit.SECONDS); // TODO expose as sys prop
                if (!succeeded) {
                    throw new DemoteFailedException("Space mode wasn't changed to be backup");
                }
                if (_spaceImpl.getSpaceMode().equals(SpaceMode.PRIMARY)) {
                    throw new DemoteFailedException("Space mode wasn't changed to backup - space still primary");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DemoteFailedException("Demote got interrupted");
            }

        } catch (DemoteFailedException e) {
            abort();
            throw e;
        } finally {
            _logger.info("Demoting to backup finished, exiting quiesce mode...");
            _spaceImpl.getQuiesceHandler().unquiesceDemote();
        }
    }

    private void validateStaticConfigurations() throws DemoteFailedException {
        if (!_spaceImpl.useZooKeeper()) {
            throw new DemoteFailedException("Primary demotion is only supported with Zookeeper leader selector.");
        }

        if (_spaceImpl.getClusterInfo().getNumberOfBackups() != 1) {
            throw new DemoteFailedException("Couldn't demote to backup - cluster should be configured with exactly one backup, backups: (" + _spaceImpl.getClusterInfo().getNumberOfBackups() + ")");
        }

        //In case that we use ZooKeeper but leader selector is not ZK based leader selector
        if (_spaceImpl.getLeaderSelector() == null || !_spaceImpl.getLeaderSelector().getClass().getName().equals(LEADER_SELECTOR_HANDLER_CLASS_NAME)) {
            throw new DemoteFailedException("Primary demotion is only supported with Zookeeper leader selector.");
        }
    }

    private void validateSpaceStatus(boolean checkQuiesce) throws DemoteFailedException {
        if (!_spaceImpl.isPrimary()) {
            //space is not primary
            throw new DemoteFailedException("Space is not primary");
        }

        if (checkQuiesce) {
            if (_spaceImpl.getQuiesceHandler().isSuspended()) {
                throw new DemoteFailedException("Space is suspended");
            }

            if (_spaceImpl.getQuiesceHandler().isQuiesced()) {
                throw new DemoteFailedException("Space is quiesced");
            }
        }

        List<ReplicationStatistics.OutgoingChannel> backupChannels = getOutgoingReplication().getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE);
        if (backupChannels.size() != 1) {
            //more than one backup
            throw new DemoteFailedException("There should be exactly one backup, current channels: (" + backupChannels.size() + ")");
        }

        ReplicationStatistics.OutgoingChannel backupChannel = backupChannels.get(0);

        if (!backupChannel.getChannelState().equals(ReplicationStatistics.ChannelState.ACTIVE)) {
            //backup replication is not active
            throw new DemoteFailedException("Backup replication channel is not active (" + backupChannel.getChannelState() + ")");
        }
    }

    private void validationChecks() throws DemoteFailedException {
        validateStaticConfigurations();

        validateSpaceStatus(true);
    }

    private ReplicationStatistics.OutgoingReplication getOutgoingReplication() {
        return _spaceImpl.getHolder().getReplicationStatistics().getOutgoingReplication();
    }

    private void closeOutgoingChannels() {
        _spaceImpl.getEngine().getReplicationNode().getAdmin().setPassive(false);
    }

    private void abort() {
        _spaceImpl.getEngine().getReplicationNode().getAdmin().setActive();
    }

    @Override
    public void beforeSpaceModeChange(SpaceMode newMode) throws RemoteException {
    }

    @Override
    public void afterSpaceModeChange(SpaceMode newMode) throws RemoteException {
        if (newMode.equals(SpaceMode.BACKUP)) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("afterSpaceModeChange >> Space mode changed to backup!");
        } else {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("afterSpaceModeChange >> Unexpected Space mode changed to " + newMode);
        }
        _latch.countDown();
    }

    private boolean isBackupSynced() {
        long lastKeyInRedoLog = getOutgoingReplication().getLastKeyInRedoLog();
        ReplicationStatistics.OutgoingChannel backupChannel = getOutgoingReplication().getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE).get(0);
        //Backup is synced
        return backupChannel.getLastConfirmedKeyFromTarget() == lastKeyInRedoLog;
    }

    public boolean waitForActiveTransactions(long timeoutInMillis) {
        try {
            return _spaceImpl.getEngine().getTransactionHandler().waitForActiveTransactions(timeoutInMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

}
