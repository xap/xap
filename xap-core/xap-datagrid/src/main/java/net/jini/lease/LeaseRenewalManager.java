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

package net.jini.lease;

import com.gigaspaces.logger.LogUtils;
import com.gigaspaces.time.SystemTime;
import com.sun.jini.config.Config;
import com.sun.jini.constants.ThrowableConstants;
import com.sun.jini.proxy.ConstrainableProxyUtil;
import com.sun.jini.thread.TaskManager;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.constraint.RemoteMethodControl;
import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseException;
import net.jini.core.lease.LeaseMap;
import net.jini.core.lease.LeaseMapException;
import net.jini.core.lease.UnknownLeaseException;

import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides for the systematic renewal and overall management of a set of leases associated with one
 * or more remote entities on behalf of a local entity. <p> This class removes much of the
 * administrative burden associated with lease renewal. Clients of the renewal manager simply give
 * their leases to the manager and the manager renews each lease as necessary to achieve a
 * <em>desired expiration</em> time (which may be later than the lease's current <em>actual
 * expiration</em> time). Failures encountered while renewing a lease can optionally be reflected to
 * the client via <code>LeaseRenewalEvent</code> instances. <p> Note that this class is not remote.
 * Entities wishing to use this class must create an instance of this class in their own virtual
 * machine to locally manage the leases granted to them. If the virtual machine that the manager was
 * created in exits or crashes, the renewal manager will be destroyed. <p> The
 * <code>LeaseRenewalManager</code> distinguishes between two time values associated with lease
 * expiration: the <em>desired expiration</em> time for the lease, and the <em>actual
 * expiration</em> time granted when the lease is created or last renewed. The desired expiration
 * represents when the client would like the lease to expire. The actual expiration represents when
 * the lease is going to expire if it is not renewed. Both time values are absolute times, not
 * relative time durations. The desired expiration time can be retrieved using the renewal manager's
 * <code>getExpiration</code> method. The actual expiration time of a lease object can be retrieved
 * by invoking the lease's <code>getExpiration</code> method. <p> Each lease in the managed set also
 * has two other associated attributes: a desired <em>renewal duration</em>, and a <em>remaining
 * desired duration</em>. The desired renewal duration is specified (directly or indirectly) when
 * the lease is added to the set. This duration must normally be a positive number; however, it may
 * be <code>Lease.ANY</code> if the lease's desired expiration is <code>Lease.FOREVER</code>. The
 * remaining desired duration is always the desired expiration less the current time. <p> Each time
 * a lease is renewed, the renewal manager will ask for an extension equal to the lease's renewal
 * duration if the renewal duration is: <ul> <li> <code>Lease.ANY</code>, or <li> less than the
 * remaining desired duration, </ul> otherwise it will ask for an extension equal to the lease's
 * remaining desired duration. <p> Once a lease is given to a lease renewal manager, the manager
 * will continue to renew the lease until one of the following occurs: <ul> <li> The lease's desired
 * or actual expiration time is reached. <li> An explicit removal of the lease from the set is
 * requested via a <code>cancel</code>, <code>clear</code>, or <code>remove</code> call on the
 * renewal manager. <li> The renewal manager tries to renew the lease and gets a bad object
 * exception, bad invocation exception, or <code>LeaseException</code>. </ul> <p> The methods of
 * this class are appropriately synchronized for concurrent operation. Additionally, this class
 * makes certain guarantees with respect to concurrency. When this class makes a remote call (for
 * example, when requesting the renewal of a lease), any invocations made on the methods of this
 * class will not be blocked. Similarly, this class makes a reentrancy guarantee with respect to the
 * listener objects registered with this class. Should this class invoke a method on a registered
 * listener (a local call), calls from that method to any other method of this class are guaranteed
 * not to result in a deadlock condition.
 *
 * @author Sun Microsystems, Inc.
 * @com.sun.jini.impl <!-- Implementation Specifics -->
 *
 * The following implementation-specific items are discussed below: <ul> <li><a
 * href="#configEntries">Configuring LeaseRenewalManager</a> <li><a href="#logging">Logging</a>
 * <li><a href="#algorithm">The renewal algorithm</a> </ul>
 *
 * <a name="configEntries"> <p><b><font size="+1">Configuring LeaseRenewalManager</font></b><p>
 * </a>
 *
 * This implementation of <code>LeaseRenewalManager</code> supports the following configuration
 * entries, with component <code>net.jini.lease.LeaseRenewalManager</code>:
 *
 * <table summary="Describes the renewBatchTimeWindow configuration entry" border="0"
 * cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1"><code>
 * renewBatchTimeWindow</code></font> <tr valign="top"> <td> &nbsp <th scope="row" align="right">
 * Type: <td> <code>long</code> <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default:
 * <td> <code>5 * 60 * 1000 // 5 minutes</code> <tr valign="top"> <td> &nbsp <th scope="row"
 * align="right"> Description: <td> The maximum number of milliseconds earlier than a lease would
 * typically be renewed to allow it to be renewed in order to permit batching its renewal with that
 * of other leases. The value must not be negative. This entry is obtained in the constructor.
 * </table> <table summary="Describes the roundTripTime configuration entry" border="0"
 * cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1"><code>
 * roundTripTime</code></font> <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type:
 * <td> <code>long</code> <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td>
 * <code>10 * 1000 // 10 seconds</code> <tr valign="top"> <td> &nbsp <th scope="row" align="right">
 * Description: <td> The worst-case latency, expressed in milliseconds, to assume for a remote call
 * to renew a lease. The value must be greater than zero. Unrealistically low values for this entry
 * may result in failure to renew a lease. Leases managed by this manager should have durations
 * exceeding the <code>roundTripTime</code>. This entry is obtained in the constructor. </table>
 * <table summary="Describes the taskManager configuration entry" border="0" cellpadding="2"> <tr
 * valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col"
 * align="left" colspan="2"> <font size="+1"><code> taskManager</code></font> <tr valign="top"> <td>
 * &nbsp <th scope="row" align="right"> Type: <td> {@link TaskManager} <tr valign="top"> <td> &nbsp
 * <th scope="row" align="right"> Default: <td> <code>new TaskManager(11, 15000, 1.0f)</code> <tr
 * valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The object used to
 * manage queuing tasks involved with renewing leases and sending notifications. The value must not
 * be <code>null</code>. The default value creates a maximum of 11 threads for performing
 * operations, waits 15 seconds before removing idle threads, and uses a load factor of 1.0 when
 * determining whether to create a new thread. Note that the implementation of the renewal algorithm
 * includes an assumption that the <code>TaskManager</code> uses a load factor of 1.0. </table>
 *
 * <a name="logging"> <p><b><font size="+1">Logging</font></b><p> </a>
 *
 * This implementation uses the {@link Logger} named <code>net.jini.lease.LeaseRenewalManager</code>
 * to log information at the following logging levels: <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the LeaseRenewalManager
 * at different logging levels">
 *
 * <caption halign="center" valign="top"><b><code> net.jini.lease.LeaseRenewalManager</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link Level#FINE FINE} <td> Lease renewal failure events, or leases that expire
 * before reaching the desired expiration time
 *
 * <tr> <td> {@link Level#FINE HANDLED} <td> Lease renewal attempts that produce indefinite
 * exceptions
 *
 * <tr> <td> {@link Level#FINE FINE} <td> Adding and removing leases, lease renewal attempts, and
 * desired lease expiration events
 *
 * </table> <p>
 *
 * <a name="algorithm"> <p><b><font size="+1">The renewal algorithm</font></b><p> </a> The time at
 * which a lease is scheduled for renewal is based on the expiration time of the lease, possibly
 * adjusted to account for the latency of the remote renewal call. The configuration entry
 * <code>roundTripTime</code>, which defaults to ten seconds, represents the total time to make the
 * remote call. <p> The following pseudocode was derived from the code which computes the renewal
 * time. In this code, <code>rtt</code> represents the value of the <code>roundTripTime</code>:
 *
 * <pre>
 *          endTime = lease.getExpiration();
 *          delta = endTime - now;
 *          if (delta <= rtt * 2) {
 * 	        delta = rtt;
 *          } else if (delta <= rtt * 8) {
 * 	        delta /= 2;
 *          } else if (delta <= 1000 * 60 * 60 * 24 * 7) {
 * 	        delta /= 8;
 *          } else if (delta <= 1000 * 60 * 60 * 24 * 14) {
 * 	        delta = 1000 * 60 * 60 * 24;
 *          } else {
 * 	        delta = 1000 * 60 * 60 * 24 * 3;
 *          }
 *          renew = endTime - delta;
 * </pre>
 *
 * It is important to note that <code>delta</code> is never less than <code>rtt</code> when the
 * renewal time is computed. A lease which would expire within this time range will be scheduled for
 * immediate renewal. The use of very short lease durations (at or below <code>rtt</code>) can cause
 * the renewal manager to effectively ignore the lease duration and repeatedly schedule the lease
 * for immediate renewal. <p> If an attempt to renew a lease fails with an indefinite exception, a
 * renewal is rescheduled with an updated renewal time as computed by the following pseudocode:
 *
 * <pre>
 *          delta = endTime - renew;
 *          if (delta > rtt) {
 *              if (delta <= rtt * 3) {
 * 	            delta = rtt;
 *              } else if (delta <= 1000 * 60 * 60) {
 * 	            delta /= 3;
 *              } else if (delta <= 1000 * 60 * 60 * 24) {
 * 	            delta = 1000 * 60 * 30;
 *              } else if (delta <= 1000 * 60 * 60 * 24 * 7) {
 * 	            delta = 1000 * 60 * 60 * 3;
 *              } else {
 * 	            delta = 1000 * 60 * 60 * 8;
 *              }
 *              renew += delta;
 *          }
 * </pre>
 *
 * Client leases are maintained in a collection sorted by descending renewal time. A renewal thread
 * is spawned whenever the renewal time of the last lease in the collection is reached. This renewal
 * thread examines all of the leases in the collection whose renewal time falls within
 * <code>renewBatchTimeWindow</code> milliseconds of the renewal time of the last lease. If any of
 * these leases can be batch renewed with the last lease (as determined by calling the {@link
 * Lease#canBatch canBatch} method of the last lease) then a {@link LeaseMap} is created, all
 * eligible leases are added to it and the {@link LeaseMap#renewAll} method is called. Otherwise,
 * the last lease is renewed directly. <p> The <code>TaskManager</code> that manages the renewal
 * threads has a bound on the number of simultaneous threads it will support. The renewal time of
 * leases may be adjusted earlier in time to reduce the likelihood that the renewal of a lease will
 * be delayed due to exhaustion of the thread pool. Actual renewal times are determined by starting
 * with the lease with the latest (farthest off) desired renewal time and working backwards.  When
 * computing the actual renewal time for a lease, the renewals of all leases with later renewal
 * times, which will be initiated during the round trip time of the current lease's renewal, are
 * considered.  If using the desired renewal time for the current lease would result in more
 * in-progress renewals than the number of threads allowed, the renewal time of the current lease is
 * shifted earlier in time, such that the maximum number of threads is not exceeded.
 * @see Lease
 * @see LeaseException
 * @see LeaseRenewalEvent
 */

@com.gigaspaces.api.InternalApi
public class LeaseRenewalManager {

    private static final long DEFAULT_RENEW_BATCH_TIME_WINDOW = 1000 * 60 * 5;
    private static final long DEFAULT_RENEWAL_RTT = 10 * 1000;

    private static final String LRM = "net.jini.lease.LeaseRenewalManager";

    private static final Logger logger = LoggerFactory.getLogger(LRM);

    /* Method objects for manipulating method constraints */
    private static final Method cancelMethod;
    private static final Method cancelAllMethod;
    private static final Method renewMethod;
    private static final Method renewAllMethod;

    static {
        try {
            cancelMethod = Lease.class.getMethod(
                    "cancel", new Class[]{});
            cancelAllMethod = LeaseMap.class.getMethod(
                    "cancelAll", new Class[]{});
            renewMethod = Lease.class.getMethod(
                    "renew", new Class[]{long.class});
            renewAllMethod = LeaseMap.class.getMethod(
                    "renewAll", new Class[]{});
        } catch (NoSuchMethodException e) {
            throw new NoSuchMethodError(e.getMessage());
        }
    }

    /* Methods for comparing lease constraints. */
    private static final Method[] leaseToLeaseMethods = {
            cancelMethod, cancelMethod, renewMethod, renewMethod
    };

    /* Methods for converting lease constraints to lease map constraints. */
    private static final Method[] leaseToLeaseMapMethods = {
            cancelMethod, cancelAllMethod, renewMethod, renewAllMethod
    };

    /**
     * Time window in which to look for batchable leases
     */
    private final long renewBatchTimeWindow;

    /**
     * Task manager for queuing and renewing leases
     */
    private final TaskManager taskManager;

    private static TaskManager createDefaultTaskManager() {
        return new TaskManager(11, 15000, 1.0F, "LeaseRenewalManager Task", 10);
    }

    /**
     * The worst-case renewal round-trip-time
     */
    private final long renewalRTT;

    /**
     * Entries for leases that are not actively being renewed. Lease with the earliest renewal is
     * last in the map.
     */
    private final SortedMap<Entry, Entry> leases = new TreeMap<Entry, Entry>();

    /**
     * Entries for leases that are actively being renewed
     */
    private final List<Entry> leaseInRenew = new ArrayList<Entry>(1);
    /**
     * The queuer task
     */
    private QueuerTask queuer = null;

    /**
     * Used to determine concurrency constraints when calculating actual renewals.  The list is
     * stored in a field to avoid reallocating it.
     */
    private List<Entry> calcList;

    private final class RenewTask implements TaskManager.Task {
        /**
         * Entries of leases to renew (if multiple, all can be batched)
         */
        private final List<Entry> bList;

        /**
         * True if this task only holds leases that have reached their actual or desired expiration
         */
        private final boolean noRenewals;

        /**
         * Create a collection of entries whose leases can be batch renewed with the last lease in
         * the map, or a list of entries whose leases need to be removed.  Which is created depends
         * on the state of the last lease in the map.  Remove each entry from the map, and add them
         * to leaseInRenew.
         */
        RenewTask(long now) {
            bList = new ArrayList<Entry>(1);
            Entry e = leases.lastKey();

            if (e.renewalsDone() || e.endTime <= now) {
                noRenewals = true;
                Map<Entry, Entry> lMap = leases.tailMap(new Entry(now, renewalRTT));
                for (Iterator<Entry> iter = lMap.values().iterator();
                     iter.hasNext(); ) {
                    Entry be = iter.next();
                    if (be.renewalsDone() || be.endTime <= now) {
                        iter.remove();
                        logExpiration(be);
            /*
             * Only add to bList if we need to tell someone
			 * about this lease's departure
			 */
                        if (be.listener != null)
                            bList.add(be);
                    }
                }
            } else {
                noRenewals = false;
                Map<Entry, Entry> lMap = leases.tailMap(
                        new Entry(e.renew + renewBatchTimeWindow, renewalRTT));
                for (Iterator<Entry> iter = lMap.values().iterator();
                     iter.hasNext(); ) {
                    Entry be = iter.next();
                    if (be == e || be.canBatch(e)) {
                        iter.remove();
                        leaseInRenew.add(be);
                        bList.add(be);
                    }
                }
            }
        }

        public void run() {
            if (noRenewals) {
                // Just notify
                tell(bList);
            } else {
		/*
		 * Get rid of any leases that have expired and then do
		 * renewals
		 */
                long now = SystemTime.timeMillis();
                List<Entry> bad = processBadLeases(now);
                if (!bList.isEmpty())
                    renewAll(bList, now);
                if (bad != null)
                    tell(bad);
            }
        }

        /**
         * No ordering.
         */
        public boolean runAfter(List tasks, int size) {
            return false;
        }

        /**
         * Find any expired leases, remove them from bList and leaseInRenew, and return any with
         * listeners.
         */
        private List<Entry> processBadLeases(long now) {
            List<Entry> bad = null;
            synchronized (LeaseRenewalManager.this) {
                for (Iterator<Entry> iter = bList.iterator(); iter.hasNext(); ) {
                    Entry e = iter.next();
                    if (e.endTime <= now) {
                        iter.remove();
                        logExpiration(e);
                        removeLeaseInRenew(e);
                        if (e.listener != null) {
                            if (bad == null)
                                bad = new ArrayList<Entry>(1);
                            bad.add(e);
                        }
                    }
                }
            }
            return bad;
        }
    }

    private static class Entry implements Comparable<Entry> {
        /*
         * Since the cnt only gets modified in the constructor, and the
         * constructor is always called from synchronized code, the cnt
         * does not need to be synchronized.
         */
        private static long cnt = 0;

        /**
         * Unique id
         */
        public final long id;
        /**
         * The lease
         */
        public final Lease lease;
        /**
         * Desired expiration
         */
        public long expiration;
        /**
         * Renew duration
         */
        public long renewDuration;
        /**
         * The listener, or null
         */
        public final LeaseListener listener;
        /**
         * Current actual expiration
         */
        public long endTime;

        private final long renewalRTT;

        /**
         * The next time we have to do something with this lease. Usually a renewal, but could be
         * removing it from the managed set because its desired expiration has been reached.
         */
        public long renew;

        /**
         * Actual time to renew, given concurrency limitations
         */
        public long actualRenew;
        /**
         * Renewal exception, or null
         */
        public Throwable ex = null;

        public Entry(Lease lease,
                     long expiration,
                     long renewDuration,
                     LeaseListener listener,
                     long renewalRTT) {
            this.endTime = lease.getExpiration();
            this.lease = lease;
            this.expiration = expiration;
            this.renewDuration = renewDuration;
            this.listener = listener;
            this.renewalRTT = renewalRTT;
            id = cnt++;
        }

        /**
         * Create a fake entry for tailMap
         */
        public Entry(long renew, long renewalRTT) {
            this.renew = renew;
            id = Long.MAX_VALUE;
            lease = null;
            listener = null;
            this.renewalRTT = renewalRTT;
        }

        /**
         * If the renewDuration is ANY, return ANY, otherwise return the minimum of the
         * renewDuration and the time remaining until the desired expiration.
         */
        public long getRenewDuration(long now) {
            if (renewDuration == Lease.ANY)
                return renewDuration;
            return Math.min(expiration - now, renewDuration);
        }

        /**
         * Calculate the renew time for the lease entry
         */
        public void calcRenew(long now) {
            endTime = lease.getExpiration();
            if (renewalsDone()) {
                if (null == desiredExpirationListener()) {
                    // Nothing urgent needs to be done with this lease
                    renew = Long.MAX_VALUE;
                } else {
		    /*
		     * Tell listener about dropping this lease in a
		     * timely fashion
		     */
                    renew = expiration;
                }
                return;
            }
            long delta = endTime - now;
            if (delta <= renewalRTT * 2) {
                delta = renewalRTT;
            } else if (delta <= renewalRTT * 8) {
                delta /= 2;
            } else if (delta <= 1000 * 60 * 60 * 24 * 7) {
                delta /= 8;
            } else if (delta <= 1000 * 60 * 60 * 24 * 14) {
                delta = 1000 * 60 * 60 * 24;
            } else {
                delta = 1000 * 60 * 60 * 24 * 3;
            }
            renew = endTime - delta;
        }

        /**
         * Calculate a new renew time due to an indefinite exception
         */
        public void delayRenew() {
            long delta = endTime - renew;
            if (delta <= renewalRTT) {
                return;
            } else if (delta <= renewalRTT * 3) {
                delta = renewalRTT;
            } else if (delta <= 1000 * 60 * 60) {
                delta /= 3;
            } else if (delta <= 1000 * 60 * 60 * 24) {
                delta = 1000 * 60 * 30;
            } else if (delta <= 1000 * 60 * 60 * 24 * 7) {
                delta = 1000 * 60 * 60 * 3;
            } else {
                delta = 1000 * 60 * 60 * 8;
            }
            renew += delta;
        }

        /**
         * Sort by decreasing renew time, secondary sort by decreasing id
         */
        public int compareTo(Entry e) {
            if (this == e)
                return 0;
            if (renew < e.renew || (renew == e.renew && id < e.id))
                return 1;
            return -1;
        }

        /**
         * Returns true if the renewal of this lease can be batched with the (earlier) renewal of
         * the given lease.  This method must be called with an entry such that e.renew <=
         * this.renew. <p>
         *
         * First checks that both leases require renewal, have the same client constraints, and can
         * be batched.  Then enforces additional requirements to avoid renewing the lease too much
         * more often than necessary. <p>
         *
         * One of the following must be true: <ul>
         *
         * <li> This lease has a renewal duration of Lease.ANY, meaning it doesn't specify its
         * renewal duration.
         *
         * <li> The amount of time from the other lease's renewal time to this one's is less than
         * half of the estimated time needed to perform renewals (renewalRTT).  In this case, the
         * renewal times are so close together that the renewal duration shouldn't be materially
         * affected.
         *
         * <li> This lease's expiration time is no more than half its renewal duration greater than
         * the renewal time of the other lease.  This case insures that this lease is not renewed
         * until at least half of it's renewal duration has elapsed. </ul> <p>
         *
         * In addition, one of the following must be true: <ul>
         *
         * <li> The other lease has a renewal duration of Lease.ANY, meaning we don't know how long
         * its next renewal will be.
         *
         * <li> The other lease is not going to be renewed again before this lease's renewal time,
         * because either its next renewal will last until after this lease's renewal time or it
         * will only be renewed once more. </ul>
         */
        public boolean canBatch(Entry e) {
            return (!renewalsDone() &&
                    !e.renewalsDone() &&
                    sameConstraints(lease, e.lease) &&
                    lease.canBatch(e.lease) &&
                    (renewDuration == Lease.ANY ||
                            renew - e.renew <= renewalRTT / 2 ||
                            endTime - e.renew <= renewDuration / 2) &&
                    (e.renewDuration == Lease.ANY ||
                            e.renew > renew - e.renewDuration ||
                            e.renew >= e.expiration - e.renewDuration));
        }

        /**
         * Returns true if the two leases both implement RemoteMethodControl and have the same
         * constraints for Lease methods, or both don't implement RemoteMethodControl, else returns
         * false.
         */
        private static boolean sameConstraints(Lease l1, Lease l2) {
            if (!(l1 instanceof RemoteMethodControl)) {
                return !(l2 instanceof RemoteMethodControl);
            } else if (!(l2 instanceof RemoteMethodControl)) {
                return false;
            } else {
                return ConstrainableProxyUtil.equivalentConstraints(
                        ((RemoteMethodControl) l1).getConstraints(),
                        ((RemoteMethodControl) l2).getConstraints(),
                        leaseToLeaseMethods);
            }
        }

        /**
         * Return the DesiredExpirationListener associated with this lease, or null if there is
         * none.
         */
        public DesiredExpirationListener desiredExpirationListener() {
            if (listener == null)
                return null;

            if (listener instanceof DesiredExpirationListener)
                return (DesiredExpirationListener) listener;

            return null;
        }

        /**
         * Return true if the actual expiration is greater than or equal to the desired expiration
         * (e.g. we don't need to renew this lease any more.
         */
        public boolean renewalsDone() {
            return expiration <= endTime;
        }
    }

    /**
     * No-argument constructor that creates an instance of this class that initially manages no
     * leases.
     */
    public LeaseRenewalManager() {
        this(DEFAULT_RENEWAL_RTT, DEFAULT_RENEW_BATCH_TIME_WINDOW);
    }

    public LeaseRenewalManager(long renewalRTT, long renewBatchTimeWindow) {
        this.renewalRTT = renewalRTT;
        this.renewBatchTimeWindow = renewBatchTimeWindow;
        this.taskManager = createDefaultTaskManager();
    }

    /**
     * Constructs an instance of this class that initially manages no leases and that uses
     * <code>config</code> to control implementation-specific details of the behavior of the
     * instance created.
     *
     * @param config supplies entries that control the configuration of this instance
     * @throws ConfigurationException if a problem occurs when obtaining entries from the
     *                                configuration
     * @throws NullPointerException   if the configuration is <code>null</code>
     */
    public LeaseRenewalManager(Configuration config)
            throws ConfigurationException {
        if (config == null)
            throw new NullPointerException("config is null");

        renewBatchTimeWindow = Config.getLongEntry(config, LRM, "renewBatchTimeWindow",
                DEFAULT_RENEW_BATCH_TIME_WINDOW, 0, Long.MAX_VALUE);
        renewalRTT = Config.getLongEntry(config, LRM, "roundTripTime",
                DEFAULT_RENEWAL_RTT, 1, Long.MAX_VALUE);
        TaskManager defaultTaskManager = createDefaultTaskManager();
        taskManager = (TaskManager) Config.getNonNullEntry(config, LRM, "taskManager",
                TaskManager.class, defaultTaskManager);
    }

    /**
     * Constructs an instance of this class that will initially manage a single lease. Employing
     * this form of the constructor is equivalent to invoking the no-argument form of the
     * constructor followed by an invocation of the three-argument form of the
     * <code>renewUntil</code> method. See <code>renewUntil</code> for details on the arguments and
     * what exceptions may be thrown by this constructor.
     *
     * @param lease             reference to the initial lease to manage
     * @param desiredExpiration the desired expiration for <code>lease</code>
     * @param listener          reference to the <code>LeaseListener</code> object that will receive
     *                          notifications of any exceptional conditions that occur during
     *                          renewal attempts. If <code>null</code> no notifications will be
     *                          sent.
     * @throws NullPointerException if <code>lease</code> is <code>null</code>
     * @see LeaseListener
     * @see #renewUntil
     */
    public LeaseRenewalManager(Lease lease,
                               long desiredExpiration,
                               LeaseListener listener) {
        this();
        renewUntil(lease, desiredExpiration, listener);
    }

    /**
     * Include a lease in the managed set until a specified time. <p> If
     * <code>desiredExpiration</code> is <code>Lease.ANY</code> calling this method is equivalent
     * the following call:
     * <pre>
     *     renewUntil(lease, Lease.FOREVER, Lease.ANY, listener)
     * </pre>
     * otherwise it is equivalent to this call:
     * <pre>
     *     renewUntil(lease, desiredExpiration, Lease.FOREVER, listener)
     * </pre>
     * <p>
     *
     * @param lease             the <code>Lease</code> to be managed
     * @param desiredExpiration when the client wants the lease to expire, in milliseconds since the
     *                          beginning of the epoch
     * @param listener          reference to the <code>LeaseListener</code> object that will receive
     *                          notifications of any exceptional conditions that occur during
     *                          renewal attempts. If <code>null</code> no notifications will be
     *                          sent.
     * @throws NullPointerException if <code>lease</code> is <code>null</code>
     * @see #renewUntil
     */
    public void renewUntil(Lease lease,
                           long desiredExpiration,
                           LeaseListener listener) {
        if (desiredExpiration == Lease.ANY) {
            renewUntil(lease, Lease.FOREVER, Lease.ANY, listener);
        } else {
            renewUntil(lease, desiredExpiration, Lease.FOREVER, listener);
        }
    }

    /**
     * Include a lease in the managed set until a specified time and with a specified renewal
     * duration. <p> This method takes as arguments: a reference to the lease to manage, the desired
     * expiration time of the lease, the renewal duration time for the lease, and a reference to the
     * <code>LeaseListener</code> object that will receive notification of exceptional conditions
     * when attempting to renew this lease. The <code>LeaseListener</code> argument may be
     * <code>null</code>. <p> If the <code>lease</code> argument is <code>null</code>, a
     * <code>NullPointerException</code> will be thrown. If the <code>desiredExpiration</code>
     * argument is <code>Lease.FOREVER</code>, the <code>renewDuration</code> argument may be
     * <code>Lease.ANY</code> or any positive value; otherwise, the <code>renewDuration</code>
     * argument must be a positive value. If the <code>renewDuration</code> argument does not meet
     * these requirements, an <code>IllegalArgumentException</code> will be thrown. <p> If the lease
     * passed to this method is already in the set of managed leases, the listener object, the
     * desired expiration, and the renewal duration associated with that lease will be replaced with
     * the new listener, desired expiration, and renewal duration. <p> The lease will remain in the
     * set until one of the following occurs: <ul> <li> The lease's desired or actual expiration
     * time is reached. <li> An explicit removal of the lease from the set is requested via a
     * <code>cancel</code>, <code>clear</code>, or <code>remove</code> call on the renewal manager.
     * <li> The renewal manager tries to renew the lease and gets a bad object exception, bad
     * invocation exception, or <code>LeaseException</code>. </ul> <p> This method will interpret
     * the value of the <code>desiredExpiration</code> argument as the desired absolute system time
     * after which the lease is no longer valid. This argument provides the ability to indicate an
     * expiration time that extends beyond the actual expiration of the lease. If the value passed
     * for this argument does indeed extend beyond the lease's actual expiration time, then the
     * lease will be systematically renewed at appropriate times until one of the conditions listed
     * above occurs. If the value is less than or equal to the actual expiration time, nothing will
     * be done to modify the time when the lease actually expires. That is, the lease will not be
     * renewed with an expiration time that is less than the actual expiration time of the lease at
     * the time of the call. <p> If the <code>LeaseListener</code> argument is a
     * non-<code>null</code> object reference, it will receive notification of exceptional
     * conditions occurring upon a renewal attempt of the lease. In particular, exceptional
     * conditions include the reception of a <code>LeaseException</code>, bad object exception, or
     * bad invocation exception (collectively these are referred to as <em>definite exceptions</em>)
     * during a renewal attempt or the lease's actual expiration being reached before its desired
     * expiration. <p> If a definite exception occurs during a lease renewal request, the exception
     * will be wrapped in an instance of the <code>LeaseRenewalEvent</code> class and sent to the
     * listener. <p> If an indefinite exception occurs during a renewal request for the lease,
     * renewal requests will continue to be made for that lease until: the lease is renewed
     * successfully, a renewal attempt results in a definite exception, or the lease's actual
     * expiration time has been exceeded. If the lease cannot be successfully renewed before its
     * actual expiration is reached, the exception associated with the most recent renewal attempt
     * will be wrapped in an instance of the <code>LeaseRenewalEvent</code> class and sent to the
     * listener. <p> If the lease's actual expiration is reached before the lease's desired
     * expiration time, and either 1) the last renewal attempt succeeded or 2) there have been no
     * renewal attempts, a <code>LeaseRenewalEvent</code> containing a <code>null</code> exception
     * will be sent to the listener.
     *
     * @param lease             the <code>Lease</code> to be managed
     * @param desiredExpiration when the client wants the lease to expire, in milliseconds since the
     *                          beginning of the epoch
     * @param renewDuration     the renewal duration to associate with the lease, in milliseconds
     * @param listener          reference to the <code>LeaseListener</code> object that will receive
     *                          notifications of any exceptional conditions that occur during
     *                          renewal attempts. If <code>null</code>, no notifications will be
     *                          sent.
     * @throws NullPointerException     if <code>lease</code> is <code>null</code>
     * @throws IllegalArgumentException if <code>renewDuration</code> is invalid
     * @see LeaseRenewalEvent
     * @see LeaseException
     */
    public void renewUntil(Lease lease,
                           long desiredExpiration,
                           long renewDuration,
                           LeaseListener listener) {
        validateDuration(renewDuration, desiredExpiration == Lease.FOREVER,
                "desiredExpiration");
        addLease(lease, desiredExpiration, renewDuration, listener,
                SystemTime.timeMillis());
    }

    /**
     * Include a lease in the managed set for a specified duration. <p> Calling this method is
     * equivalent the following call:
     * <pre>
     *     renewFor(lease, desiredDuration, Lease.FOREVER, listener)
     * </pre>
     *
     * @param lease           reference to the new lease to manage
     * @param desiredDuration the desired duration (relative time) that the caller wants
     *                        <code>lease</code> to be valid for, in milliseconds
     * @param listener        reference to the <code>LeaseListener</code> object that will receive
     *                        notifications of any exceptional conditions that occur during renewal
     *                        attempts. If <code>null</code>, no notifications will be sent.
     * @throws NullPointerException if <code>lease</code> is <code>null</code>
     * @see #renewFor
     */
    public void renewFor(Lease lease, long desiredDuration,
                         LeaseListener listener) {
        renewFor(lease, desiredDuration, Lease.FOREVER, listener);
    }

    /**
     * Include a lease in the managed set for a specified duration and with specified renewal
     * duration. <p> The semantics of this method are similar to those of the four-argument form of
     * <code>renewUntil</code>, with <code>desiredDuration</code> + current time being used for the
     * value of the <code>desiredExpiration</code> argument of <code>renewUntil</code>. The only
     * exception to this is that, in the context of <code>renewFor</code>, the value of the
     * <code>renewDuration</code> argument may only be <code>Lease.ANY</code> if the value of the
     * <code>desiredDuration</code> argument is <em>exactly</em> <code>Lease.FOREVER.</code> <p>
     * This method tests for arithmetic overflow in the desired expiration time computed from the
     * value of <code>desiredDuration</code> argument (<code>desiredDuration</code> + current time).
     * Should such overflow be present, a value of <code>Lease.FOREVER</code> is used to represent
     * the lease's desired expiration time.
     *
     * @param lease           reference to the new lease to manage
     * @param desiredDuration the desired duration (relative time) that the caller wants
     *                        <code>lease</code> to be valid for, in milliseconds
     * @param renewDuration   the renewal duration to associate with the lease, in milliseconds
     * @param listener        reference to the <code>LeaseListener</code> object that will receive
     *                        notifications of any exceptional conditions that occur during renewal
     *                        attempts. If <code>null</code>, no notifications will be sent.
     * @throws NullPointerException     if <code>lease</code> is <code>null</code>
     * @throws IllegalArgumentException if <code>renewDuration</code> is invalid
     * @see #renewUntil
     */
    public void renewFor(Lease lease,
                         long desiredDuration,
                         long renewDuration,
                         LeaseListener listener) {
	/*
	 * Validate before calculating effective desiredExpiration, if
	 * they want a renewDuration of Lease.ANY, desiredDuration has
	 * to be exactly Lease.FOREVER
	 */
        validateDuration(renewDuration, desiredDuration == Lease.FOREVER,
                "desiredDuration");

        long now = SystemTime.timeMillis();
        long desiredExpiration;
        if (desiredDuration < Lease.FOREVER - now) { // check overflow.
            desiredExpiration = now + desiredDuration;
        } else {
            desiredExpiration = Lease.FOREVER;
        }
        addLease(lease, desiredExpiration, renewDuration, listener, now);
    }

    /**
     * Error checking function that ensures renewDuration is valid taking into account the whether
     * or not the desired expiration/duration is Lease.FOREVER. Throws an appropriate
     * IllegalArgumentException if an invalid renewDuration is passed.
     *
     * @param renewDuration renew duration the clients wants
     * @param isForever     should be true if client asked for a desired expiration/duration of
     *                      exactly Lease.FOREVER
     * @param name          name of the desired expiration/duration field, used to construct
     *                      exception
     * @throws IllegalArgumentException if renewDuration is invalid
     */
    private void validateDuration(long renewDuration, boolean isForever,
                                  String name) {
        if (renewDuration <= 0 &&
                !(renewDuration == Lease.ANY && isForever)) {
	    /*
	     * A negative renew duration and is not lease.ANY with a
	     * forever desired expiration
	     */
            if (renewDuration == Lease.ANY) {
		/*
		 * Must have been Lease.ANY with a non-FOREVER desired
		 * expiration
		 */
                throw new IllegalArgumentException("A renewDuration of " +
                        "Lease.ANY can only be used with a " + name + " of " +
                        "Lease.FOREVER");
            }

            if (isForever) {
                // Must have been a non-Lease.ANY, non-positive renewDuration
                throw new IllegalArgumentException("When " + name + " is " +
                        "Lease.FOREVER the only valid values for renewDuration " +
                        "are a positive number, Lease.ANY, or Lease.FOREVER");
            }

	    /*
	     * Must be a non-positive renewDuration with a non-Forever 
	     * desired expiration
	     */
            throw new IllegalArgumentException("When the " + name +
                    " is not Lease.FOREVER the only valid values for " +
                    "renewDuration are a positive number or Lease.FOREVER");
        }
    }

    private synchronized void addLease(Lease lease,
                                       long desiredExpiration,
                                       long renewDuration,
                                       LeaseListener listener,
                                       long now) {
        Entry e = findEntryDo(lease);
        if (e != null && !removeLeaseInRenew(e))
            leases.remove(e);
        insertEntry(new Entry(lease, desiredExpiration, renewDuration,
                        listener, renewalRTT),
                now);
        calcActualRenews(now);
        logger.debug("Added lease {0}", lease);
    }

    /**
     * Calculate the preferred renew time, and put in the map
     */
    private void insertEntry(Entry e, long now) {
        e.calcRenew(now);
        leases.put(e, e);
    }

    /**
     * Returns the current desired expiration time associated with a particular lease, (not the
     * actual expiration that was granted when the lease was created or last renewed).
     *
     * @param lease the lease the caller wants the current desired expiration for
     * @return a <code>long</code> value corresponding to the current desired expiration time
     * associated with <code>lease</code>
     * @throws UnknownLeaseException if the lease passed to this method is not in the set of managed
     *                               leases
     * @see UnknownLeaseException
     * @see #setExpiration
     */
    public synchronized long getExpiration(Lease lease)
            throws UnknownLeaseException {
        return findEntry(lease).expiration;
    }

    /**
     * Replaces the current desired expiration of a given lease from the managed set with a new
     * desired expiration time. <p> Note that an invocation of this method with a lease that is
     * currently a member of the managed set is equivalent to an invocation of the
     * <code>renewUntil</code> method with the lease's current listener as that method's
     * <code>listener</code> argument. Specifically, if the value of the <code>expiration</code>
     * argument is less than or equal to the lease's current desired expiration, this method takes
     * no action.
     *
     * @param lease      the lease whose desired expiration time should be replaced
     * @param expiration <code>long</code> value representing the new desired expiration time for
     *                   the <code>lease</code> argument
     * @throws UnknownLeaseException if the lease passed to this method is not in the set of managed
     *                               leases
     * @see #renewUntil
     * @see UnknownLeaseException
     * @see #getExpiration
     */
    public synchronized void setExpiration(Lease lease, long expiration)
            throws UnknownLeaseException {
        Entry e = findEntry(lease);
        e.expiration = expiration;
        if (expiration != Lease.FOREVER && e.renewDuration == Lease.ANY)
            e.renewDuration = Lease.FOREVER;
        if (leaseInRenew.indexOf(e) < 0) {
            leases.remove(e);
            long now = SystemTime.timeMillis();
            insertEntry(e, now);
            calcActualRenews(now);
        }
    }

    /**
     * Removes a given lease from the managed set, and cancels it. <p> Note that even if an
     * exception is thrown as a result of the cancel operation, the lease will still have been
     * removed from the set of leases managed by this class. Additionally, any exception thrown by
     * the <code>cancel</code> method of the lease object itself may also be thrown by this method.
     *
     * @param lease the lease to remove and cancel
     * @throws UnknownLeaseException if the lease passed to this method is not in the set of managed
     *                               leases
     * @throws RemoteException       typically, this exception occurs when there is a communication
     *                               failure between the client and the server. When this exception
     *                               does occur, the lease may or may not have been successfully
     *                               cancelled, (but the lease is guaranteed to have been removed
     *                               from the managed set).
     * @see Lease#cancel
     * @see UnknownLeaseException
     */
    public void cancel(Lease lease)
            throws UnknownLeaseException, RemoteException {
        remove(lease);
        lease.cancel();
    }

    /**
     * Removes a given lease from the managed set of leases; but does not cancel the given lease.
     *
     * @param lease the lease to remove from the managed set
     * @throws UnknownLeaseException if the lease passed to this method is not in the set of managed
     *                               leases
     * @see UnknownLeaseException
     */
    public synchronized void remove(Lease lease) throws UnknownLeaseException {
        Entry e = findEntry(lease);
        if (!removeLeaseInRenew(e))
            leases.remove(e);
        calcActualRenews();
        logger.debug("Removed lease {0}", lease);
    }

    /**
     * Removes all leases from the managed set of leases. This method does not request the
     * cancellation of the removed leases.
     */
    public synchronized void clear() {
        leases.clear();
        leaseInRenew.clear();
        calcActualRenews();
        logger.debug("Removed all leases");
    }

    public synchronized void terminate() {
        if (taskManager.isTerminated()) {
            return;
        }
        clear();
        taskManager.terminate();
    }

    /**
     * Calculate the actual renew times, and poke/restart the queuer
     */
    private void calcActualRenews() {
        calcActualRenews(SystemTime.timeMillis());
    }

    /**
     * Calculate the actual renew times, and poke/restart the queuer
     */
    private void calcActualRenews(long now) {
	/*
	 * Subtract one to account for the queuer thread, which should not be
	 * counted.
	 */
        int maxThreads = taskManager.getMaxThreads() - 1;
        if (calcList == null) {
            calcList = new ArrayList(maxThreads);
        }
        for (Iterator<Entry> iter = leases.values().iterator(); iter.hasNext(); ) {
            Entry e = iter.next();

            // Start by assuming we can renew the lease when we want
            e.actualRenew = e.renew;

            if (e.renewalsDone()) {
		/*
		 * The lease's actual expiration is >= desired
		 * expiration, drop the lease if the desired expiration
		 * has been reached and we don't have to tell anyone
		 * about it
		 */
                if (now >= e.expiration &&
                        e.desiredExpirationListener() == null) {
                    logExpiration(e);
                    iter.remove();
                }

		/*
		 * Even if we have to send an event we assume that it
		 * won't consume a slot in our schedule
		 */
                continue;
            }

            if (e.endTime <= now && e.listener == null) {
                // Lease has expired and no listener, just remove it.
                logExpiration(e);
                iter.remove();
                continue;
            }

	    /*
	     * Make sure there aren't too many lease renewal threads
	     * operating at the same time.
	     */
            if (!canBatch(e)) {
		/*
		 * Find all renewals that start before we expect ours to
		 * be done.
		 */
                for (Iterator<Entry> listIter = calcList.iterator();
                     listIter.hasNext(); ) {
                    if (e.renew >= listIter.next().actualRenew - renewalRTT) {
			/*
			 * This renewal starts after we expect ours to
			 * be done.
			 */
                        break;
                    }
                    listIter.remove();
                }
                if (calcList.size() == maxThreads) {
		    /*
		     * Too many renewals.  Move our actual renewal time
		     * earlier so we'll probably be done before the last
		     * one needs to start.  Remove that one, since it
		     * won't overlap any earlier renewals.
		     */
                    Entry e1 = calcList.remove(0);
                    e.actualRenew = e1.actualRenew - renewalRTT;
                }
                calcList.add(e);
            }
        }
        calcList.clear();
        long newWakeup = wakeupTime();
        if (queuer == null) {
            if (newWakeup < Long.MAX_VALUE) {
                queuer = new QueuerTask(newWakeup);
                taskManager.add(queuer);
            }
        } else if (newWakeup < queuer.wakeup ||
                (newWakeup == Long.MAX_VALUE && leaseInRenew.isEmpty())) {
            notifyAll();
        }
    }

    /**
     * Return true if e can be batched with another entry that expires between e.renew -
     * renewBatchTimeWindow and e.renew.
     */
    private boolean canBatch(Entry e) {
        Iterator<Entry> iter = leases.tailMap(e).values().iterator();
        iter.next(); // skip e itself
        while (iter.hasNext()) {
            Entry be = iter.next();
            if (e.renew - be.renew > renewBatchTimeWindow)
                break;
            if (e.canBatch(be))
                return true;
        }
        return false;
    }

    /**
     * Find a lease entry, throw exception if not found or expired normally
     */
    private Entry findEntry(Lease lease) throws UnknownLeaseException {
        Entry e = findEntryDo(lease);
        if (e != null &&
                (e.renew < e.endTime || SystemTime.timeMillis() < e.endTime)) {
            return e;
        }
        throw new UnknownLeaseException();
    }

    /**
     * Find a lease entry, or null
     */
    private Entry findEntryDo(Lease lease) {
        Entry e = findLeaseFromIterator(leases.values().iterator(), lease);
        if (e == null)
            e = findLeaseFromIterator(leaseInRenew.iterator(), lease);
        return e;
    }

    /**
     * Find a lease entry, or null
     */
    private static Entry findLeaseFromIterator(Iterator<Entry> iter, Lease lease) {
        while (iter.hasNext()) {
            Entry e = iter.next();
            if (e.lease.equals(lease))
                return e;
        }
        return null;
    }

    /**
     * Notify the listener for each lease
     */
    private void tell(List<Entry> bad) {
        for (Iterator<Entry> iter = bad.iterator(); iter.hasNext(); ) {
            Entry e = iter.next();
            if (e.renewalsDone()) {
                final DesiredExpirationListener del =
                        e.desiredExpirationListener();
                if (del != null) {
                    del.expirationReached(new LeaseRenewalEvent(this, e.lease,
                            e.expiration, null));
                }
                continue;
            }
            e.listener.notify(new LeaseRenewalEvent(this, e.lease,
                    e.expiration, e.ex));
        }
    }

    /**
     * Logs a lease expiration, distinguishing between expected and premature expirations.
     *
     * @param e the <code>Entry</code> holding the lease
     */
    private void logExpiration(Entry e) {
        if (e.renewalsDone()) {
            logger.debug(
                    "Reached desired expiration for lease {0}",
                    e.lease);
        } else {
            logger.debug(
                    "Lease {0} expired before reaching desired expiration", e.lease);
        }
    }

    /**
     * Renew all of the leases (if multiple, all can be batched)
     */
    private void renewAll(List<Entry> bList, long now) {
        Map<Lease, Throwable> lmeMap = null;
        Throwable t = null;
        List<Entry> bad = null;

        try {
            if (bList.size() == 1) {
                Entry e = bList.get(0);
                logger.debug("Renewing lease {0}", e.lease);
                e.lease.renew(e.getRenewDuration(now));
            } else {
                LeaseMap batchLeaseMap = createBatchLeaseMap(bList, now);
                logger.debug("Renewing leases {0}", batchLeaseMap);
                batchLeaseMap.renewAll();
            }
        } catch (LeaseMapException ex) {
            lmeMap = ex.exceptionMap;
            bad = new ArrayList<Entry>(ex.exceptionMap.size());
        } catch (Throwable ex) {
            t = ex;
            bad = new ArrayList<Entry>(bList.size());  // They may all be bad
        }

	/*
	 * For each lease we tried to renew determine the associated
	 * exception (if any), and then ether add the lease back to
	 * leases (if the renewal was successful), schedule a retry and
	 * add back to leases (if the renewal was indefinite), or drop
	 * the lease (by not adding it back to leases) and notify any
	 * interested listeners.  In any event remove lease from the
	 * list of leases being renewed.
	 */

        now = SystemTime.timeMillis();
        synchronized (this) {
            for (Iterator<Entry> iter = bList.iterator(); iter.hasNext(); ) {
                Entry e = iter.next();

                if (!removeLeaseInRenew(e))
                    continue;

                // Update the entries exception field
                if (bad == null) {
                    e.ex = null;
                } else {
                    e.ex = (t != null) ? t : lmeMap.get(e.lease);
                }

                if (e.ex == null) {
                    // No problems just put back in list
                    insertEntry(e, now);
                    continue;
                }

		/*
		 * Some sort of problem.  If definite don't put back
		 * into leases and setup to notify the appropriate
		 * listener, if indefinite schedule for a retry and put
		 * back into leases
		 */
                final int cat = ThrowableConstants.retryable(e.ex);
                if (cat == ThrowableConstants.INDEFINITE) {
                    e.delayRenew();
                    leases.put(e, e);
                    if (logger.isDebugEnabled()) {
                        LogUtils.throwing(logger, LeaseRenewalManager.class, "renewAll", e.ex, "Indefinite exception while renewing lease {0}", e.lease);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        LogUtils.throwing(logger, LeaseRenewalManager.class, "renewAll", e.ex, "Lease renewal failed for lease {0}", e.lease);
                    }
                    if (e.listener != null) {
			/*
			 * Note: For us ThrowableConstants.UNCATEGORIZED ==
			 * definite
			 */
                        bad.add(e);
                    }
                }
            }
            calcActualRenews(now);
        }

        if (bad != null)
            tell(bad);
    }

    /**
     * Create a LeaseMap for batch renewal
     */
    private static LeaseMap createBatchLeaseMap(List<Entry> bList, long now) {
        Iterator<Entry> iter = bList.iterator();
        Entry e = iter.next();
        LeaseMap batchLeaseMap =
                e.lease.createLeaseMap(e.getRenewDuration(now));
        if (e.lease instanceof RemoteMethodControl &&
                batchLeaseMap instanceof RemoteMethodControl) {
            batchLeaseMap = (LeaseMap)
                    ((RemoteMethodControl) batchLeaseMap).setConstraints(
                            ConstrainableProxyUtil.translateConstraints(
                                    ((RemoteMethodControl) e.lease).getConstraints(),
                                    leaseToLeaseMapMethods));
        }
        while (iter.hasNext()) {
            e = iter.next();
            batchLeaseMap.put(e.lease, new Long(e.getRenewDuration(now)));
        }
        return batchLeaseMap;
    }

    /**
     * Remove from leaseInRenew, return true if removed
     */
    private boolean removeLeaseInRenew(Entry e) {
        int index = leaseInRenew.indexOf(e); // avoid iterator cons
        if (index < 0)
            return false;
        leaseInRenew.remove(index);
        return true;
    }

    /**
     * Return the soonest actual renewal time
     */
    private long wakeupTime() {
        if (leases.isEmpty())
            return Long.MAX_VALUE;
        return leases.lastKey().actualRenew;
    }

    private class QueuerTask implements TaskManager.Task {

        /**
         * When to next wake up and queue a new renew task
         */
        long wakeup;

        QueuerTask(long wakeup) {
            this.wakeup = wakeup;
        }

        /**
         * No ordering
         */
        public boolean runAfter(List tasks, int size) {
            return false;
        }

        public void run() {
            synchronized (LeaseRenewalManager.this) {
                try {
                    while (true) {
                        wakeup = wakeupTime();
                        if (wakeup == Long.MAX_VALUE && leaseInRenew.isEmpty())
                            break;
                        final long now = SystemTime.timeMillis();
                        long delta = wakeup - now;
                        if (delta <= 0) {
                            taskManager.add(new RenewTask(now));
                        } else {
                            LeaseRenewalManager.this.wait(delta);
                        }
                    }
                } catch (InterruptedException ex) {
                }
                queuer = null;
            }
        }
    }
}
