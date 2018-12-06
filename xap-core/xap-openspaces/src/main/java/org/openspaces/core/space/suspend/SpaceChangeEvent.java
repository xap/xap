package org.openspaces.core.space.suspend;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.server.space.suspend.SuspendType;
import com.j_spaces.core.IJSpace;
import org.springframework.context.ApplicationEvent;

public class SpaceChangeEvent extends ApplicationEvent {

    private static final long serialVersionUID = 1L;
    private final SpaceMode spaceMode;

    private SuspendType suspendType;

    /**
     * Creates a new Space suspend type changed event.
     *  @param space     The space that changed its suspend type
     * @param suspendType The current suspend type of the space
     * @param currentSpaceMode
     */
    public SpaceChangeEvent(IJSpace space, SuspendType suspendType, SpaceMode currentSpaceMode) {
        super(space);
        this.suspendType = suspendType;
        this.spaceMode = currentSpaceMode;
    }

    /**
     * Returns the space that initiated this event.
     */
    public IJSpace getSpace() {
        return (IJSpace) getSource();
    }

    public SuspendType getSuspendType() {
        return this.suspendType;
    }

    public SpaceMode getSpaceMode() {
        return spaceMode;
    }
}
