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

package com.gigaspaces.internal.lease;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.internal.DefaultAsyncResult;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.operations.UpdateLeasesSpaceOperationRequest;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.exception.internal.InterruptedSpaceException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class LeaseUtils {
    public static final long DISCARD_LEASE = -1;
    private static final long DUMMY_TTL_FOR_EXPIRED_ENTRIES = 1;

    public static long toExpiration(long duration) {
        long expiration = duration + SystemTime.timeMillis();
        return expiration < 0 ? Long.MAX_VALUE : expiration;
    }

    public static long safeAdd(long x, long y) {
        // Safe way to express: x + y > Long.MAX_VALUE ? Long.MAX_VALUE : x + y;
        return x > Long.MAX_VALUE - y ? Long.MAX_VALUE : x + y;
    }

    public static long getTimeToLive(long expirationTime, boolean useDummyIfRelevant) {
        final long timeToLive = expirationTime != Long.MAX_VALUE ? (expirationTime - SystemTime.timeMillis()) : expirationTime;
        return (useDummyIfRelevant && timeToLive <= 0 && expirationTime != Long.MAX_VALUE) ? DUMMY_TTL_FOR_EXPIRED_ENTRIES : timeToLive;
    }

    public static Map<SpaceLease, Throwable> updateBatch(IDirectSpaceProxy spaceProxy, LeaseUpdateBatch batch) {
        UpdateLeasesSpaceOperationRequest request = new UpdateLeasesSpaceOperationRequest(batch.getLeasesUpdateDetails(),
                batch.isRenew());
        final long updateTime = batch.isRenew() ? SystemTime.timeMillis() : 0;
        try {
            spaceProxy.getProxyRouter().execute(request);
        } catch (InterruptedException e) {
            throw new InterruptedSpaceException(e);
        }

        Exception[] errors = request.getFinalResult();
        return processResult(errors, batch, updateTime);
    }

    public static void updateBatchAsync(IDirectSpaceProxy spaceProxy, LeaseUpdateBatch batch, AsyncFutureListener<Map<SpaceLease, Throwable>> listener) {
        UpdateLeasesSpaceOperationRequest request = new UpdateLeasesSpaceOperationRequest(
                batch.getLeasesUpdateDetails(), batch.isRenew());

        spaceProxy.getProxyRouter().executeAsync(request, new ListenerWrapper(batch, listener));
    }

    private static Map<SpaceLease, Throwable> processResult(Exception[] errors, LeaseUpdateBatch batch,
                                                            long updateTime) {
        Map<SpaceLease, Throwable> result = errors == null ? null : new HashMap<SpaceLease, Throwable>(errors.length);
        if (errors == null) {
            if (batch.isRenew())
                for (int i = 0; i < batch.getSize(); i++)
                    batch.getLeases()[i]._expirationTime = safeAdd(updateTime, batch.getLeasesUpdateDetails()[i].getRenewDuration());
        } else {
            for (int i = 0; i < batch.getSize(); i++) {
                if (errors[i] != null)
                    result.put(batch.getLeases()[i], errors[i]);
                else if (batch.isRenew())
                    batch.getLeases()[i]._expirationTime = safeAdd(updateTime, batch.getLeasesUpdateDetails()[i].getRenewDuration());
            }
        }

        return result;
    }

    private static class ListenerWrapper implements AsyncFutureListener<Object> {
        private final LeaseUpdateBatch batch;
        private final long updateTime;
        private final AsyncFutureListener<Map<SpaceLease, Throwable>> listener;

        public ListenerWrapper(LeaseUpdateBatch batch, AsyncFutureListener<Map<SpaceLease, Throwable>> listener) {
            this.batch = batch;
            this.updateTime = batch.isRenew() ? SystemTime.timeMillis() : 0;
            this.listener = listener;
        }

        @Override
        public void onResult(AsyncResult<Object> result) {
            Exception error = result.getException();
            Map<SpaceLease, Throwable> map = null;
            if (error == null)
                map = processResult((Exception[]) result.getResult(), batch, updateTime);
            if (listener != null)
                listener.onResult(new DefaultAsyncResult<Map<SpaceLease, Throwable>>(map, error));
        }
    }
}
