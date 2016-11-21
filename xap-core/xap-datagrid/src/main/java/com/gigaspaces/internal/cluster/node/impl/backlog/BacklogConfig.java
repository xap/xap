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

package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.j_spaces.core.cluster.SwapBacklogConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures a {@link IReplicationGroupBacklog}
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class BacklogConfig {
    /**
     * Specifies the way to act when the configured limit of the backlog is reached
     *
     * @author eitany
     */
    public enum LimitReachedPolicy {
        DROP_OLDEST((byte) 0),
        DROP_UNTIL_RESYNC((byte) 1),
        BLOCK_NEW((byte) 2),
        DROP_MEMBER((byte) 3); //Since 8.0.5

        private final byte _code;

        private LimitReachedPolicy(byte code) {
            _code = code;
        }

        public static LimitReachedPolicy fromCode(byte code) {
            switch (code) {
                case 0:
                    return DROP_OLDEST;
                case 1:
                    return DROP_UNTIL_RESYNC;
                case 2:
                    return BLOCK_NEW;
                case 3:
                    return DROP_MEMBER;
                default:
                    throw new IllegalArgumentException("illegal code " + code);
            }
        }

        public byte getCode() {
            return _code;
        }

    }

    public static final long UNLIMITED = -1;
    protected static final LimitReachedPolicy DEFAULT_BACKLOG_REACHED_POLICY = LimitReachedPolicy.BLOCK_NEW;

    private final Map<String, Long> _memberLimitDuringSynchronization = new HashMap<String, Long>();
    private final Map<String, Long> _membersLimit = new HashMap<String, Long>();
    private final Map<String, LimitReachedPolicy> _membersLimitReachedPolicy = new HashMap<String, BacklogConfig.LimitReachedPolicy>();
    private final Map<String, LimitReachedPolicy> _memberLimitDuringSynchronizationReachedPolicy = new HashMap<String, BacklogConfig.LimitReachedPolicy>();

    private int _limitedMemoryCapacity = (int) UNLIMITED;
    private SwapBacklogConfig _swapBacklogConfig = new SwapBacklogConfig();

    private BacklogWeightPolicy _backlogWeightPolicy = BacklogWeightPolicyFactory.create("weight-by-packets");

    public void setLimit(String memberLookupName, long limit, LimitReachedPolicy limitReachedPolicy) {
        _membersLimit.put(memberLookupName, limit);
        _membersLimitReachedPolicy.put(memberLookupName, limitReachedPolicy);
    }

    public long getLimit(String memberLookupName) {
        Long memberLimit = _membersLimit.get(memberLookupName);
        if (memberLimit == null)
            return UNLIMITED;

        return memberLimit.longValue();
    }

    public void setUnlimited(String memberLookupName) {
        _membersLimit.remove(memberLookupName);
        _membersLimitReachedPolicy.remove(memberLookupName);
    }

    public boolean isLimited(String memberLookupName) {
        return _membersLimit.containsKey(memberLookupName);
    }

    public LimitReachedPolicy getLimitReachedPolicy(String memberLookupName) {
        LimitReachedPolicy limitReachedPolicy = _membersLimitReachedPolicy.get(memberLookupName);
        if (limitReachedPolicy != null)
            return limitReachedPolicy;

        return DEFAULT_BACKLOG_REACHED_POLICY;
    }

    public void setUnlimitedDuringSynchronization(String memberName) {
        _memberLimitDuringSynchronization.remove(memberName);
        _memberLimitDuringSynchronizationReachedPolicy.remove(memberName);
    }

    public boolean isLimitedDuringSynchronization(String memberName) {
        return _memberLimitDuringSynchronization.containsKey(memberName);
    }

    public void setLimitDuringSynchronization(String memberName, long limit, LimitReachedPolicy limitReachedPolicy) {
        _memberLimitDuringSynchronization.put(memberName, limit);
        _memberLimitDuringSynchronizationReachedPolicy.put(memberName, limitReachedPolicy);
    }

    public long getLimitDuringSynchronization(String memberName) {
        Long limit = _memberLimitDuringSynchronization.get(memberName);
        if (limit == null)
            return UNLIMITED;

        return limit.longValue();
    }

    public LimitReachedPolicy getLimitDuringSynchronizationReachedPolicy(String memberLookupName) {
        LimitReachedPolicy limitReachedPolicy = _memberLimitDuringSynchronizationReachedPolicy.get(memberLookupName);
        if (limitReachedPolicy != null)
            return limitReachedPolicy;

        return DEFAULT_BACKLOG_REACHED_POLICY;
    }

    public void setMemberBacklogLimitation(String memberName, BacklogMemberLimitationConfig memberLimitationConfig) {
        validate(memberName, memberLimitationConfig);
        if (memberLimitationConfig.isLimited())
            setLimit(memberName, memberLimitationConfig.getLimit(), memberLimitationConfig.getLimitReachedPolicy());
        else
            setUnlimited(memberName);

        if (memberLimitationConfig.isLimitedDuringSynchronization())
            setLimitDuringSynchronization(memberName, memberLimitationConfig.getLimitDuringSynchronization(), memberLimitationConfig.getLimitReachedPolicyDuringSynchronization());
        else
            setUnlimitedDuringSynchronization(memberName);
    }

    private void validate(String memberName, BacklogMemberLimitationConfig memberLimitationConfig) {
        if (!memberLimitationConfig.isLimitedDuringSynchronization())
            return;
        //limit during recovery should only be higher or equal to limit during regular state.
        if (!memberLimitationConfig.isLimited() || memberLimitationConfig.getLimit() > memberLimitationConfig.getLimitDuringSynchronization())
            throw new IllegalStateException("Illegal replication backlog limitations configuration for member ["
                    + memberName
                    + "], cannot specify backlog limitation during synchronization process ["
                    + limitationToString(memberLimitationConfig.getLimitDuringSynchronization())
                    + "] which is lower than the global limitations ["
                    + limitationToString(memberLimitationConfig.getLimit())
                    + "]");

    }

    private String limitationToString(long limitDuringSynchronization) {
        return "" + (limitDuringSynchronization == UNLIMITED ? "UNLIMITED" : limitDuringSynchronization);
    }

    public BacklogMemberLimitationConfig getBacklogMemberLimitation(
            String memberName) {
        BacklogMemberLimitationConfig config = new BacklogMemberLimitationConfig();
        if (isLimited(memberName))
            config.setLimit(getLimit(memberName), getLimitReachedPolicy(memberName));
        else
            config.setUnlimited();

        if (isLimitedDuringSynchronization(memberName))
            config.setLimitDuringSynchronization(getLimitDuringSynchronization(memberName), getLimitDuringSynchronizationReachedPolicy(memberName));
        else
            config.setUnlimitedDuringSynchronization();

        return config;
    }

    public boolean isLimitedMemoryCapacity() {
        return _limitedMemoryCapacity != UNLIMITED;
    }

    public void setLimitedMemoryCapacity(int limitedMemoryCapacity) {
        _limitedMemoryCapacity = limitedMemoryCapacity;
    }

    public int getLimitedMemoryCapacity() {
        return _limitedMemoryCapacity;
    }

    public void setUnlimitedMemoryCapacity() {
        _limitedMemoryCapacity = (int) UNLIMITED;
    }

    public void setSwapBacklogConfig(SwapBacklogConfig swapBacklogConfig) {
        _swapBacklogConfig = swapBacklogConfig;
    }

    public SwapBacklogConfig getSwapBacklogConfig() {
        return _swapBacklogConfig;
    }

    public BacklogConfig clone() {
        BacklogConfig clone = new BacklogConfig();
        clone.overrideWithOther(this);
        return clone;
    }

    public void removeMemberConfig(String memberName) {
        _memberLimitDuringSynchronization.remove(memberName);
        _membersLimit.remove(memberName);
        _membersLimitReachedPolicy.remove(memberName);
        _memberLimitDuringSynchronizationReachedPolicy.remove(memberName);

    }

    protected void overrideWithOther(BacklogConfig other) {
        _membersLimit.putAll(other._membersLimit);
        _memberLimitDuringSynchronization.putAll(other._memberLimitDuringSynchronization);
        _membersLimitReachedPolicy.putAll(other._membersLimitReachedPolicy);
        _memberLimitDuringSynchronizationReachedPolicy.putAll(other._memberLimitDuringSynchronizationReachedPolicy);

        setLimitedMemoryCapacity(other.getLimitedMemoryCapacity());
        setSwapBacklogConfig(other.getSwapBacklogConfig());
    }

    @Override
    public String toString() {
        return "BacklogConfig [_memberLimitDuringSynchronization="
                + _memberLimitDuringSynchronization + ", _membersLimit="
                + _membersLimit + ", _membersLimitReachedPolicy="
                + _membersLimitReachedPolicy
                + ", _memberLimitDuringSynchronizationReachedPolicy="
                + _memberLimitDuringSynchronizationReachedPolicy
                + ", _limitedMemoryCapacity=" + _limitedMemoryCapacity
                + ", _swapBacklogConfig=" + _swapBacklogConfig + "]";
    }

    public BacklogWeightPolicy getBackLogWeightPolicy() {
        return _backlogWeightPolicy;
    }

    public void setBackLogSizePolicy(String policy) {
        _backlogWeightPolicy = BacklogWeightPolicyFactory.create(policy);
    }
}
