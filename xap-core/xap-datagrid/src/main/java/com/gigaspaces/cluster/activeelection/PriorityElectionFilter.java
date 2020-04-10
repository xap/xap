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

package com.gigaspaces.cluster.activeelection;

import com.gigaspaces.cluster.activeelection.core.ActiveElectionState;
import com.gigaspaces.cluster.activeelection.core.IActiveElectionDecisionFilter;
import com.j_spaces.core.ISpaceState;
import com.j_spaces.core.JSpaceState;
import com.j_spaces.core.cluster.FailOverPolicy;
import com.j_spaces.lookup.entry.ContainerName;
import com.j_spaces.lookup.entry.State;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.entry.Name;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This election decision filter provides the condition to aquire desired space state on
 * NamingService. The state will be acquired only if {@link #isAcceptable(ActiveElectionState.State,
 * List)} returns true. <p> The final Primary space status will be acquired only if
 * ActiveElectionState.State.ACTIVE will be accepted. <p> NOTE: The member with {@link
 * ISpaceState#STARTED} of {@link com.j_spaces.lookup.entry.State} or with lower order memberID in
 * fail-over group is acceptable to advance to <code>aquireState</code>.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @since 6.0
 **/
@com.gigaspaces.api.InternalApi
public class PriorityElectionFilter
        implements IActiveElectionDecisionFilter {
    final private static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLUSTER_ACTIVE_ELECTION);

    private final String _memberName;
    private final FailOverPolicy _failOverPolicy;

    /**
     * Constructor.
     *
     * @param memberName     the cluster member name.
     * @param failOverPolicy the fail over policy.
     **/
    public PriorityElectionFilter(String memberName, FailOverPolicy failOverPolicy) {
        _memberName = memberName;
        _failOverPolicy = failOverPolicy;
    }

    /**
     * This method provides the condition to advance the manage member to <code>aquireState</code>
     * state.
     *
     * @param candidateSrv the candidates to aquire desired state.
     * @param aquireState  the state to aquire.
     * @return <code>true</code> if the manage member is acceptable to aquire desired
     * <code>aquireState</code> state.
     **/
    @Override
    public boolean isAcceptable(ActiveElectionState.State aquireState, List<ServiceItem> candidateSrv) {
        final String STARTED_STATE = JSpaceState.convertToString(ISpaceState.STARTED);

	  /* prepare sorted candidate list */
        List<CandidateEntry> orderedMemberList = new ArrayList<CandidateEntry>();
        for (ServiceItem si : candidateSrv) {
            String memberName = getMemberName(si.attributeSets);
            String state = getMemberState(memberName, si.attributeSets);

            orderedMemberList.add(new CandidateEntry(memberName, STARTED_STATE.equals(state)));
        }

        Collections.sort(orderedMemberList);

	  /* get the orderId from the elected candidate list */
        int orderId = orderedMemberList.indexOf(new CandidateEntry(_memberName));

        if (orderId == -1 && _logger.isErrorEnabled()) {
            String msgEx = "Cluster may be in inconsistent state due to an illegal active election state: Space instance <" + _memberName + "> is not registered as a candidate to become PRIMARY in LUS.\n Please restart the space cluster. ";

            _logger.error(msgEx);
            return false;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug("PriorityElectionFilter - " +
                    "\n Member: [" + _memberName + "] ask to acquire [" + aquireState + "] state." +
                    "\n OrderId: " + orderId +
                    "\n Candidates: " + orderedMemberList +
                    "\n Acquire accepted: " + (orderId == 0));
        }
	 
	  /* acceptable to aquire the state, only if index==0 */
        return orderId == 0;
    }

    /**
     * @return the member {@link State} registered on Lookup Service
     */
    private String getMemberState(String memberName, Entry[] spaceAttr) {
        for (Entry e1 : spaceAttr) {
            if (e1 instanceof State)
                return ((State) e1).state;
        }

        throw new IllegalArgumentException("Space: " + memberName + " registered on LookupService without " + State.class.getName() + " lookup attribute.");
    }

    /**
     * @return the cluster member name constructed from the registered space attributes
     */
    private String getMemberName(Entry[] spaceAttr) {
        String spaceName = null;
        String containerName = null;

        for (Entry e1 : spaceAttr) {
            if (spaceName == null && e1 instanceof Name)
                spaceName = ((Name) e1).name;

            if (containerName == null && e1 instanceof ContainerName)
                containerName = ((ContainerName) e1).name;
        }

        if (spaceName == null || containerName == null)
            throw new IllegalArgumentException("Space registered on LookupService with a wrong LookupAttributes: [Name=" + spaceName + ", ContainerName=" + containerName + "]");

        return containerName + ":" + spaceName;
    }

    @Override
    public int compareTo(ServiceItem service1, ServiceItem service2) {
        ICandidateEntry entry1 = getCandidateEntry(service1);
        ICandidateEntry entry2 = getCandidateEntry(service2);

        return entry1.compareTo(entry2);

    }

    @Override
    public ICandidateEntry getCandidateEntry(ServiceItem service) {
        final String STARTED_STATE = JSpaceState.convertToString(ISpaceState.STARTED);

        String memberName = getMemberName(service.attributeSets);
        String state = getMemberState(memberName, service.attributeSets);

        return new CandidateEntry(memberName, STARTED_STATE.equals(state));
    }

    /**
     * Comparable class to sort candidate members according the {@link State} and order ID in the
     * cluster. The member with {@link ISpaceState#STARTED} of {@link com.j_spaces.lookup.entry.State}
     * or lower order memberID in fail-over group is acceptable to advance to
     * <code>acquireState</code>.
     **/
    final public class CandidateEntry
            implements ICandidateEntry {
        final private String memberName;

        @Override
        public String getMemberName() {
            return memberName;
        }

        @Override
        public boolean isStarted() {
            return isStarted;
        }

        final private boolean isStarted;

        /**
         * constructor only for List.get() method
         */
        private CandidateEntry(String memberName) {
            this(memberName, false);
        }

        private CandidateEntry(String memberName, boolean isStarted) {
            this.memberName = memberName;
            this.isStarted = isStarted;
        }

        /* (non-Javadoc)
       * @see com.gigaspaces.cluster.activeelection.ICandidateEntry#compareTo(com.gigaspaces.cluster.activeelection.PriorityElectionFilter.CandidateEntry)
       */
        @Override
        public int compareTo(ICandidateEntry currCandidate) {
            if (currCandidate.isStarted() && !isStarted)
                return 1;

            if (!currCandidate.isStarted() && isStarted)
                return -1;

            int memberOrderIndex = _failOverPolicy.failOverGroupMembersNames.indexOf(memberName);
            int candidateOrderIndex = _failOverPolicy.failOverGroupMembersNames.indexOf(currCandidate.getMemberName());

            return memberOrderIndex - candidateOrderIndex;
        }

        @Override
        public boolean equals(Object obj) {
            return memberName.equals(((CandidateEntry) obj).memberName);
        }

        @Override
        public String toString() {
            return "[ MemberName: " + memberName + ", Started: " + isStarted + " ]\n";
        }
    }
}// end
