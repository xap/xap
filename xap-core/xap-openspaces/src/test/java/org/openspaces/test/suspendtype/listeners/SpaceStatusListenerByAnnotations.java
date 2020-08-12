package org.openspaces.test.suspendtype.listeners;

import org.openspaces.core.space.status.SpaceStatusChanged;
import org.openspaces.core.space.status.SpaceStatusChangedEvent;

/**
 * @author Elad Gur
 */
public class SpaceStatusListenerByAnnotations extends AbstractSpaceStatusChangedListener {

    @SpaceStatusChanged
    public void onSpaceStatusChanged(SpaceStatusChangedEvent event) {
        storeEvent(event);
    }

}
