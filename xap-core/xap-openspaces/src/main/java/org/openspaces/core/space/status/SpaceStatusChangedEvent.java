package org.openspaces.core.space.status;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.server.space.suspend.SuspendType;
import com.j_spaces.core.IJSpace;
import org.springframework.context.ApplicationEvent;

public class SpaceStatusChangedEvent extends ApplicationEvent {

    private static final long serialVersionUID = 1L;
    private final SpaceMode spaceMode;
    private final SuspendType suspendType;

    /**
     * Creates a new Space status changed event that occurred when a space change his {@link SuspendType} or {@link SpaceMode}
     *
     * @param space       - The space that changed its status
     * @param suspendType - The current suspend type of the space
     * @param spaceMode   - The current space mode
     */
    public SpaceStatusChangedEvent(IJSpace space, SuspendType suspendType, SpaceMode spaceMode) {
        super(space);
        this.suspendType = suspendType;
        this.spaceMode = spaceMode;
    }

    public SuspendType getSuspendType() {
        return this.suspendType;
    }

    public SpaceMode getSpaceMode() {
        return spaceMode;
    }

    /**
     * Returns true if the space instance is primary and not suspended, false otherwise.
     * @since 14.5
     */
    public boolean isActive() {
        return spaceMode.equals(SpaceMode.PRIMARY) && suspendType.equals(SuspendType.NONE);
    }
}
