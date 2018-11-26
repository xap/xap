package org.openspaces.core.space.suspend;

import com.gigaspaces.server.space.suspend.SuspendType;
import com.j_spaces.core.IJSpace;
import org.springframework.context.ApplicationEvent;

public class SuspendTypeChangedEvent extends ApplicationEvent {

    private static final long serialVersionUID = 1L;

    private SuspendType suspendType;

    /**
     * Creates a new Space suspend type changed event.
     *
     * @param space     The space that changed its suspend type
     * @param suspendType The current suspend type of the space
     */
    public SuspendTypeChangedEvent(IJSpace space, SuspendType suspendType) {
        super(space);
        this.suspendType = suspendType;
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

}
