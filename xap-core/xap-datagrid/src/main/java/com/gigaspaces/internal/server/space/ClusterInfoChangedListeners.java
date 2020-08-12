package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.cluster.SpaceClusterInfo;

import java.util.ArrayList;
import java.util.List;

/***
 *   @author yael nahon
 *   @since 15.5.0
 */
@com.gigaspaces.api.InternalApi
public class ClusterInfoChangedListeners {

    private final List<IClusterInfoChangedListener> listeners = new ArrayList<>();

    public synchronized void afterClusterInfoChange(SpaceClusterInfo clusterInfo) {
        for (IClusterInfoChangedListener listener : listeners)
            listener.afterClusterInfoChange(clusterInfo);

    }

    public synchronized void addListener(IClusterInfoChangedListener listener) {
        listeners.add(listener);
    }

    public synchronized void clear() {
        listeners.clear();
    }

    public boolean isEmpty() {
        return listeners.isEmpty();
    }

    public void removeListener(IClusterInfoChangedListener listener) {
        this.listeners.remove(listener);
    }
}
