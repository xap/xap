package org.openspaces.test.suspendtype.listeners;

import org.openspaces.core.space.suspend.anntations.SuspendTypeChanged;
import org.openspaces.core.space.suspend.SpaceChangeEvent;

/**
 * @author Elad Gur
 */
public class SpaceSuspendTypeChangedListenerByAnnotations extends AbstractSpaceSuspendTypeChangedListener {

    @SuspendTypeChanged
    public void onSuspendInfoChanged(SpaceChangeEvent event) {
        storeEvent(event);
    }

}
