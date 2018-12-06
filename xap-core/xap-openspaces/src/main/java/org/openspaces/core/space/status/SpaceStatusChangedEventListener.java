package org.openspaces.core.space.status;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.server.space.suspend.SuspendType;

/**
 * Interface receiving events when a space {@link SuspendType} or {@link SpaceMode} change
 *
 * @author Elad Gur
 * @since  14.0.1
 */
public interface SpaceStatusChangedEventListener {

    /**
     * @param event - an {@link SpaceStatusChangedEvent} that contain the {@link SuspendType} and the {@link SpaceMode}}
     */
    void onSpaceStatusChanged(SpaceStatusChangedEvent event);

}