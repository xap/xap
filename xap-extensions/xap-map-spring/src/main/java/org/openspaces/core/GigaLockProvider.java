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
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.CannotCreateTransactionException;

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

    private IJSpace space;
    private final ConcurrentHashMap<String, TxWithTimeStamp> lockedUIDHashMap = new ConcurrentHashMap<String, TxWithTimeStamp>();
    private long lockTimeToLive;
    private long waitingForLockTime;
    private final DistributedTransactionManagerProvider transactionManagerProvider;

    /**
     *
     * @param space     space proxy
     * @param lockTimeToLive default lock life time in milliseconds
     * @param waitingForLockTime default timeout for acquiring a lock
     */
    public GigaLockProvider(IJSpace space, long lockTimeToLive, long waitingForLockTime) {
        this.space = space;
        this.lockTimeToLive = lockTimeToLive;
        this.waitingForLockTime = waitingForLockTime;
        try {
            transactionManagerProvider = new DistributedTransactionManagerProvider();
        } catch (TransactionException e) {
            throw new CannotCreateTransactionException("Failed to obtain transaction lock manager", e);
        }
    }
    /**
     * Lock lifetime and lock acquistion timeout are default values
     *
     * @param space     space proxy
     */
    public GigaLockProvider(IJSpace space) {
        this.space = space;
        this.lockTimeToLive = DEFAULT_LOCK_TIME_TO_LIVE;
        this.waitingForLockTime = DEFAULT_WAITING_TIME_FOR_LOCK;

        try {
            transactionManagerProvider = new DistributedTransactionManagerProvider();
        } catch (TransactionException e) {
            throw new CannotCreateTransactionException("Failed to obtain transaction lock manager", e);
        }
    }

    /**
     * Instantly locks an object with default lock lifetime.
     *
     * @param object    object to lock
     * @return true if lock success, false otherwise
     */
    public boolean tryLock(Object object) {
        return lock(object, lockTimeToLive, 0);
    }

    /**
     * Instantly locks an object with provided lock lifetime.
     *
     * @param object        object to lock
     * @param lockTimeToLive
     * @return true if lock success, false otherwise
     */
    public boolean tryLock(Object object, long lockTimeToLive) {
        return lock(object, lockTimeToLive, 0);
    }

    /**
     * Locks an object with default lock lifetime and default timeout waiting for lock
     *
     * @param   object
     * @return  true if lock success, false otherwise
     */
    public boolean acquireLock(Object object) {
        return lock(object, lockTimeToLive, waitingForLockTime);
    }
    /**
     * Locks an object with provided lock lifetime and timeout waiting for lock
     *
     * @param   object
     * @return  true if lock success, false otherwise
     */
    public boolean acquireLock(Object object, long lockTimeToLive, long waitingForLockTime) {
        return lock(object, lockTimeToLive, waitingForLockTime);
    }

    /**
     Locking of object under tx. Locking is done using space write operation
     */
    private boolean lock(Object object, long lockTimeToLive, long waitingForLockTime) {
        String uid = String.valueOf(object);
        Transaction tr = null;
        try {
            tr = getTransaction(lockTimeToLive);
        } finally {
            if (tr == null) {
                lockedUIDHashMap.remove(uid);
                return false;
            }
        }

        try {
            space.write(new ObjectEnvelope(object), tr, Lease.FOREVER, waitingForLockTime, Modifiers.UPDATE_OR_WRITE);
        } catch (SpaceTimeoutException e) {
            try {
                tr.abort();
            } catch (Exception re) {
                logger.warn("Failed to abort transaction", e);
            }
            // rethrow
            throw e;
        } catch (Throwable t) {
            try {
                tr.abort();
            } catch (Exception re) {
                logger.warn("Failed to abort transaction", t);
            }
            lockedUIDHashMap.remove(uid);
            throw new DataAccessResourceFailureException("Failed to obtain lock for object [" + object + "]", t);
        }

        //otherwise, map uid->txn
        lockedUIDHashMap.put(uid, new TxWithTimeStamp(tr,lockTimeToLive));

        return true;

    }

    /**
     * Release a locked object
     * @param object
     * @throws Exception if lock release fails
     */
    public void releaseLock(Object object) throws Exception {
        String uid = String.valueOf(object);
        if (lockedUIDHashMap.containsKey(uid)) {
            Transaction tr = lockedUIDHashMap.get(uid).tx;
            try {
                tr.abort();
            } catch (Exception e) {
                logger.warn("Failed to abort the lock transaction, object is still locked", e);
                throw e;
            }
        }
    }
    /**
     * Forced release of a locked object
     * @param object
     * @throws Exception if lock release fails
     */
    public void deleteLock(Object object) {
        try{
            releaseLock(object);
        }catch(Exception e){
            logger.warn("Failed to unlock the object, ignoring", e);
        }finally {
            lockedUIDHashMap.remove(String.valueOf(object));
        }
    }

    public boolean isLocked(Object object) {
        String uid = String.valueOf(object);
        //Looks locally for the locked object
        if (isObjectLockedLocally(object))
            return true;
        //Looks remotely for the locked object
        if (isEntryLockedRemotely(object))
            return true;

        return false;
    }

    private boolean isEntryLockedRemotely(Object object) {
        try {
            return space.read(new ObjectEnvelope(object), null, 0, Modifiers.DIRTY_READ)!=null;
        } catch (Exception e) {

        }

        return false;
    }

    private boolean isObjectLockedLocally(Object object) {
        String uid = String.valueOf(object);

        if(lockedUIDHashMap.containsKey(uid)){
            return lockedUIDHashMap.get(uid).isTxAlive();
        }

        return false;
    }

    private Transaction getTransaction(long lockTimeToLive){
        Transaction.Created tCreated;
        try {
            tCreated = TransactionFactory.create(transactionManagerProvider.getTransactionManager(), lockTimeToLive);
        } catch (Exception e) {
            throw new CannotCreateTransactionException("Failed to create lock transaction", e);
        }
        return tCreated.transaction;
    }
    /*
    Helper class that holds the transaction with timestamp and timeout. Object is locked only during timeout period
     */
    private class TxWithTimeStamp{
        Transaction tx;
        long startTime;
        long timeout;


        public TxWithTimeStamp(Transaction tx, long timeout) {
            this.tx = tx;
            this.startTime = System.currentTimeMillis();
            this.timeout = timeout;
        }

        public boolean isTxAlive(){
            return System.currentTimeMillis() > (startTime + timeout);
        }
    }
    /*
    Creates a pojo out of the object to lock
     */
    private static class ObjectEnvelope {

        private Object object;

        public ObjectEnvelope(){
            this.object = null;
        }

        public ObjectEnvelope(Object object){
            this.object = object;
        }

        @SpaceId
        public Object getObject() {
            return object;
        }

        public void setObject(Object object) {
            this.object = object;
        }
    }


}
