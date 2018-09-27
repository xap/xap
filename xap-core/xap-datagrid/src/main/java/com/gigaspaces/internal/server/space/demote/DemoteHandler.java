package com.gigaspaces.internal.server.space.demote;

import com.gigaspaces.admin.quiesce.DefaultQuiesceToken;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.filters.ReplicationStatistics;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.LeaderSelector.LEADER_SELECTOR_HANDLER_CLASS_NAME;


/**
 * @author Yohana Khoury
 * @since 14.0
 */
@com.gigaspaces.api.InternalApi
public class DemoteHandler {
    private final Logger _logger;
    private final SpaceImpl _spaceImpl;
    private volatile boolean isDemoteInProgress = false;

    public DemoteHandler(SpaceImpl spaceImpl) {
        _spaceImpl = spaceImpl;
        _logger = Logger.getLogger(Constants.LOGGER_DEMOTE+ '.' + spaceImpl.getNodeName());

    }

    public void demote(int timeToWait, TimeUnit unit) throws DemoteFailedException {
//        _spaceImpl.beforeOperation(true, false /*checkQuiesceMode*/, null);
//        _spaceImpl.getQuiesceHandler().suspend("nnnn");

        isDemoteInProgress = true;
        demoteImpl(timeToWait,unit);
        isDemoteInProgress = false;
    }

    public void demoteImpl(int timeToWait, TimeUnit unit) throws DemoteFailedException {
        long timeToWaitInMs = unit.toMillis(timeToWait);
        long end = timeToWaitInMs + System.currentTimeMillis();

        if (!_spaceImpl.isPrimary()) {
            //space is not primary
            throw new DemoteFailedException("Space is not primary");
        }

        validateCanProgress();

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

        List<ReplicationStatistics.OutgoingChannel> backupChannels = _spaceImpl.getHolder().getReplicationStatistics().getOutgoingReplication().getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE);
        if (backupChannels.size() != 1) {
            //more than one backup
            throw new DemoteFailedException("There should be exactly one backup, current channels: ("+backupChannels.size()+")");
        }

        ReplicationStatistics.OutgoingChannel backupChannel = backupChannels.get(0);

        if (!backupChannel.getChannelState().equals(ReplicationStatistics.ChannelState.ACTIVE)) {
            //backup replication is not active
            throw new DemoteFailedException("Backup replication channel is not active ("+backupChannel.getChannelState()+")");
        }


        try {

            //TODO quiesce token - replace with empty token
            _logger.info("Demoting to backup, entering quiesce mode...");
            _spaceImpl.getQuiesceHandler().quiesce("Space is demoting from primary to backup", new DefaultQuiesceToken("myToken"));


            long remainingTime = end - System.currentTimeMillis();
            boolean leaseManagerCycleFinished = _spaceImpl.getEngine().getLeaseManager().waitForNoCycleOnQuiesce(remainingTime);
            if (!leaseManagerCycleFinished) {
                throw new DemoteFailedException("Couldn't demote to backup - lease manager cycle timeout");
            }

            remainingTime = end - System.currentTimeMillis();

            if (remainingTime <= 0) {
                throw new DemoteFailedException("Couldn't demote to backup - timeout waiting for a lease manager cycle");
            }

//            remainingTime = end - System.currentTimeMillis();
//            _spaceImpl.getEngine().getTransactionHandler().abortOpenTransactions();
//            _spaceImpl.getEngine().getTransactionHandler().waitForActiveTransactions(remainingTime);

            remainingTime = end - System.currentTimeMillis();
            if (remainingTime <= 0) {
                throw new DemoteFailedException("Couldn't demote to backup - timeout while waiting for active transactions");
            }


            //Sleep for the remaining time
            try {
                Thread.sleep(remainingTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            long lastKeyInRedoLog = _spaceImpl.getHolder().getReplicationStatistics().getOutgoingReplication().getLastKeyInRedoLog();

            if (_spaceImpl.getHolder().getReplicationStatistics().getOutgoingReplication().getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE).get(0).getLastConfirmedKeyFromTarget() != lastKeyInRedoLog) {
                //Backup is not synced
                throw new DemoteFailedException("Couldn't demote to backup - backup is not synced ("+backupChannel.getLastConfirmedKeyFromTarget()+" != "+lastKeyInRedoLog+")");
            }

            _logger.info("size during demote = "+_spaceImpl.getHolder().getReplicationStatistics().getOutgoingReplication().getRedoLogSize());
            //TODO recheck above preconditions again after entering quiesce mode
            //Wait timeout if necessary (e.g. wait for replication from primary to backup)

            // wait for no activity
            //TODO
            validateCanProgress();
            //close outgoing connections
            closeChannels();

            //Verify that there was no issues against ZK like suspend, disconnect
            // If already suspended, abort
            validateCanProgress();

            //Restart leader selector handler (in case of ZK then it restarts
            // the connection to ZK so the backup becomes primary)
            //If suspend happens, we need to clean the suspend after the demote!
            if (!_spaceImpl.restartLeaderSelectorHandler()) {
                throw new DemoteFailedException("Could not restart leader selector");
            }

        } catch (DemoteFailedException e) {
            abort();
            throw e;
        } finally {
            _logger.info("Demoting to backup finished, exiting quiesce mode...");
            _spaceImpl.getQuiesceHandler().unquiesce();
        }
    }

    public boolean isDemoteInProgress() {
        return isDemoteInProgress;
    }

    private boolean isSuspended() {
        return _spaceImpl.getQuiesceHandler().isSuspended();
    }


    private void closeChannels() {
        _spaceImpl.getEngine().getReplicationNode().getAdmin().setPassive(false);
    }

    private void validateCanProgress() throws DemoteFailedException {
        if (isSuspended()) {
            throw new DemoteFailedException(ERR_SPACE_IS_SUSPENDED);
        }
//        if (isQuiesced()) {
//            throw new DemoteFailedException(ERR_SPACE_IS_QUIESCED);
//        }
    }

    private boolean isQuiesced() {
        return _spaceImpl.getQuiesceHandler().isQuiesced();
    }


    private void abort() {
        _spaceImpl.getEngine().getReplicationNode().getAdmin().setActive();

    }

  public final String ERR_SPACE_IS_SUSPENDED = "Space is suspended";
  public final String ERR_SPACE_IS_QUIESCED = "Space is quiesced";

}
