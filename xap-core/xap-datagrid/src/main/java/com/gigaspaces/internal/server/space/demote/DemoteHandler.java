package com.gigaspaces.internal.server.space.demote;

import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.filters.ReplicationStatistics;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    private CountDownLatch _latch;
    private final int _minTimeToDemoteInMs;
    private static String MIN_TIME_TO_DEMOTE_IN_MS = "demote_handler.min_time_for_demote";

    public DemoteHandler(SpaceImpl spaceImpl) {
        _spaceImpl = spaceImpl;
        _logger = Logger.getLogger(Constants.LOGGER_DEMOTE + '.' + spaceImpl.getNodeName());
        _minTimeToDemoteInMs = _spaceImpl.getConfigReader().getIntSpaceProperty(MIN_TIME_TO_DEMOTE_IN_MS, "5000");
    }

    public void demote(int timeout, TimeUnit unit) throws DemoteFailedException {
        if (unit.toMillis(timeout) < _minTimeToDemoteInMs) {
            throw new DemoteFailedException("Timeout must be equal or greater than " + MIN_TIME_TO_DEMOTE_IN_MS + "=" + _minTimeToDemoteInMs + "ms");
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


    private long tryWithinTimeout(String msg, long timeoutMs, Function<Long, Boolean> f) throws TimeoutException {
        long start = System.currentTimeMillis();
        Boolean success = f.apply(timeoutMs);
        if (!success) {
            throw new TimeoutException(msg);
        }

        long duration = System.currentTimeMillis() - start;
        long remainingTime = timeoutMs - duration;
        if (remainingTime < 0) {
            throw new TimeoutException(msg);
        }

        return remainingTime;
    }

    private long repetitiveTryWithinTimeout(String msg, long timeoutMs, Callable<Boolean> f) throws TimeoutException, DemoteFailedException {
        long start = System.currentTimeMillis();
        long remainingTime;
        while ((remainingTime = timeoutMs - (System.currentTimeMillis() - start)) > 0) {
            try {
                Boolean success = f.call();
                if (success) return remainingTime;
            } catch (Exception e) {
                //ignore
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new DemoteFailedException("Demote got interrupted");
            }
        }

        throw new TimeoutException(msg);
    }

    private void demoteImpl(int timeout, TimeUnit timeoutUnit) throws DemoteFailedException, TimeoutException {
        long start = System.currentTimeMillis();
        long timeoutMs = timeoutUnit.toMillis(timeout);

        try {

            _logger.info("Demoting to backup, entering quiesce mode...");
            _spaceImpl.getQuiesceHandler().quiesceDemote("Space is demoting from primary to backup");


            long remainingTime = timeoutMs;
            remainingTime = tryWithinTimeout("Couldn't demote to backup - lease manager cycle timeout", remainingTime, (innerTimeout) ->
                    _spaceImpl.getEngine().getLeaseManager().waitForNoCycleOnQuiesce(innerTimeout)
            );


            remainingTime = tryWithinTimeout("Couldn't demote to backup - timeout while waiting for transactions", remainingTime, (innerTimeout) ->
//                _spaceImpl.getEngine().getTransactionHandler().waitForActiveTransactions(innerTimeout);
                            true
            );


            //Sleep remaining time to minTimeToDemoteInMs
            long currentDuration = System.currentTimeMillis() - start;
            if (currentDuration < _minTimeToDemoteInMs) {
                try {
                    Thread.sleep(_minTimeToDemoteInMs - currentDuration);
                } catch (InterruptedException e) {
                    throw new DemoteFailedException("Demote got interrupted");
                }
            }

            repetitiveTryWithinTimeout("Backup is not synced", remainingTime, () -> {
                long lastKeyInRedoLog = getOutgoingReplication().getLastKeyInRedoLog();
                ReplicationStatistics.OutgoingChannel backupChannel = getOutgoingReplication().getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE).get(0);
                if (backupChannel.getLastConfirmedKeyFromTarget() == lastKeyInRedoLog) {
                    return true; //Backup is synced
                } else {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine("Backup is not synced (" + backupChannel.getLastConfirmedKeyFromTarget() + " != " + lastKeyInRedoLog + ")");
                    return false;
                }
            });


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
}
