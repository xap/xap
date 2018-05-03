package org.openspaces.core;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.client.transaction.DistributedTransactionManagerProvider;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.Modifiers;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.TransactionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.space.SpaceProxyConfigurer;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides a distriubted lock, using built-in transaction locking.
 *
 * @author Alon Shoham
 */
public class GigaLockProvider {
    private final static Log logger = LogFactory.getLog(GigaLockProvider.class);
    private final static long DEFAULT_LOCK_TIME_TO_LIVE = 60000;
    private final static long DEFAULT_WAITING_TIME_FOR_LOCK = 10000;

    private final IJSpace space;
    private final ConcurrentHashMap<Serializable, LockInfo> locksCache = new ConcurrentHashMap<Serializable, LockInfo>();
    private final long lockTimeToLive;
    private final long waitingForLockTime;
    private final DistributedTransactionManagerProvider transactionManagerProvider;

    /**
     *
     * @param spaceProxyConfigurer
     * @param lockTimeToLive default lock life time in milliseconds
     * @param waitingForLockTime default timeout for acquiring a lock
     */
    public GigaLockProvider(SpaceProxyConfigurer spaceProxyConfigurer, long lockTimeToLive, long waitingForLockTime) {
        this.space = spaceProxyConfigurer.create();
        this.lockTimeToLive = lockTimeToLive;
        this.waitingForLockTime = waitingForLockTime;
        try {
            transactionManagerProvider = new DistributedTransactionManagerProvider();
        } catch (TransactionException e) {
            throw new GigaLockException("Failed to obtain transaction lock manager", e);
        }
    }
    /**
     * Lock lifetime and lock acquistion timeout are default values
     *
     * @param spaceProxyConfigurer
     */
    public GigaLockProvider(SpaceProxyConfigurer spaceProxyConfigurer) {
        this(spaceProxyConfigurer, DEFAULT_LOCK_TIME_TO_LIVE, DEFAULT_WAITING_TIME_FOR_LOCK);
    }

    /**
     * Instantly locks an object with default lock lifetime.
     *
     * @param key    object to lock
     * @return true if lock success, false otherwise
     */
    public boolean tryLock(Serializable key) {
        return lock(key, lockTimeToLive, 0);
    }

    /**
     * Instantly locks an object with provided lock lifetime.
     *
     * @param key        object to lock
     * @param lockTimeToLive
     * @return true if lock success, false otherwise
     */
    public boolean tryLock(Serializable key, long lockTimeToLive) {
        return lock(key, lockTimeToLive, 0);
    }

    /**
     * Locks an object with default lock lifetime and default timeout waiting for lock
     *
     * @param   key
     * @return  true if lock success, false otherwise
     */
    public boolean acquireLock(Serializable key) {
        return lock(key, lockTimeToLive, waitingForLockTime);
    }
    /**
     * Locks an object with provided lock lifetime and timeout waiting for lock
     *
     * @param   key
     * @return  true if lock success, false otherwise
     */
    public boolean acquireLock(Serializable key, long lockTimeToLive, long waitingForLockTime) {
        return lock(key, lockTimeToLive, waitingForLockTime);
    }

    /**
     Locking of object under tx. Locking is done using space write operation
     */
    private boolean lock(Serializable key, long lockTimeToLive, long waitingForLockTime) {
        final Transaction tr = getTransaction(lockTimeToLive);

        try {
            space.write(new LockEntry(key), tr, Lease.FOREVER, waitingForLockTime, Modifiers.UPDATE_OR_WRITE);
            locksCache.put(key, new LockInfo(tr,lockTimeToLive));
            return true;
        } catch (SpaceTimeoutException e) {
            try {
                tr.abort();
            } catch (Exception re) {
                logger.warn("Failed to abort transaction", re);
            }
            return false;
        } catch (Throwable t) {
            try {
                tr.abort();
            } catch (Exception re) {
                logger.warn("Failed to abort transaction", re);
            }
            throw new GigaLockException("Failed to obtain lock for key [" + key + "]", t);
        }
    }

    /**
     * Release a locked object
     * @param key
     * @throws Exception if lock release fails
     */
    public void releaseLock(Serializable key){
        LockInfo lockInfo = locksCache.get(key);

        if (lockInfo != null) {
            try {
                lockInfo.tx.abort();
                locksCache.remove(key);
            } catch (Exception e) {
                throw new GigaLockException("Failed to release lock for entry " + key, e);
            }
        }
        else{
            if(isEntryLockedRemotely(key)){
                throw new GigaLockException("Cannot release entry which is locked by another client");
            }
        }
    }

    public boolean isLocked(Serializable key) {
        //Looks locally for the locked object
        if (isObjectLockedLocally(key))
            return true;
        //Looks remotely for the locked object
        if (isEntryLockedRemotely(key))
            return true;

        return false;
    }

    private boolean isEntryLockedRemotely(Serializable key){
        try {
            return space.read(new LockEntry(key), null, 0, Modifiers.DIRTY_READ)!=null;
        } catch (Exception e) {
           throw new GigaLockException("failed to determine if entry "+ key + " is locked by another client", e);
        }
    }

    private boolean isObjectLockedLocally(Serializable key) {
        LockInfo lockInfo = locksCache.get(key);

        return lockInfo != null ? lockInfo.isTxAlive() : false;
    }

    private Transaction getTransaction(long lockTimeToLive){
        Transaction.Created tCreated;
        try {
            tCreated = TransactionFactory.create(transactionManagerProvider.getTransactionManager(), lockTimeToLive);
        } catch (Exception e) {
            throw new GigaLockException("Failed to create lock transaction", e);
        }
        return tCreated.transaction;
    }
    /*
    Helper class that holds the transaction with timestamp and timeout. Object is locked only during timeout period
     */
    private class LockInfo {
        final Transaction tx;
        private final long expirationTime;


        public LockInfo(Transaction tx, long timeout) {
            this.tx = tx;
            this.expirationTime = System.currentTimeMillis() + timeout;
        }

        public boolean isTxAlive(){
            return System.currentTimeMillis() < expirationTime;
        }
    }
    /*
    Creates a pojo out of the object to lock
     */
    private static class LockEntry {

        private Serializable key;

        public LockEntry(){

        }

        public LockEntry(Serializable key){
            this.key = key;
        }

        @SpaceId
        public Serializable getKey() {
            return key;
        }

        public void setKey(Serializable key) {
            this.key = key;
        }
    }
}
